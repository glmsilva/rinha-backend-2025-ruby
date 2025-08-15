# frozen_string_literal: true

require 'guaraci'
require 'async'
require 'db/client'
require 'db/postgres'
require 'async/job'
require 'async/http'
require 'net/http'

DEFAULT_PROCESSOR_URL = 'http://payment-processor-default:8080'
FALLBACK_PROCESSOR_URL = 'http://payment-processor-fallback:8080'
CLIENT = DB::Client.new(DB::Postgres::Adapter.new(
                          user: 'postgres',
                          password: 'password',
                          database: 'rinha_db',
                          port: 5432,
                          host: 'postgres'
                        ))

class DefaultHealthcheck
  def initialize(check_interval: 5)
    @check_interval = check_interval
    @healthcheck_url = DEFAULT_PROCESSOR_URL.to_s
    @last_check_time = nil
    @last_status = nil
    @checking = false
  end

  def healthy?
    Console.logger.info("Iniciando checagem do Healthcheck: #{should_check?}  ")
    # return true if @last_check_time.nil?

    check_health if should_check?

    @last_status.nil? || @last_status
    # @last_status || false
  end

  private

  def should_check?
    return true if @last_check_time.nil?
    return false if @checking

    Console.logger.info("Ultima checagem foi há #{Process.clock_gettime(Process::CLOCK_MONOTONIC) - @last_check_time} segundos")
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - @last_check_time > @check_interval
  end

  def check_health
    return if @checking

    @checking = true
    # Async do
    begin
      uri = URI.parse(@healthcheck_url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.get('/payments/service-health') do |response|
        @last_status = !JSON.parse(response)['failing']
        @last_check_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        Console.logger.info("Healthcheck status: #{@last_status ? 'De boa' : 'Nao ta de boa'}  ")
      end
    rescue StandardError => e
      @last_status = false
      @last_check_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      Console.logger.error("Healthcheck failed: #{e.message}")
    end
    # ensure
    # @checking = false
    # end
  end
end

class AsyncPaymentQueue
  def initialize(max_workers: 5, healthcheck: nil, session: nil)
    @queue = Async::Queue.new
    @fallback_queue = Async::Queue.new
    @workers = []
    @running = true
    @max_workers = max_workers
    @healthcheck = healthcheck
    @session = session
  end

  def start
    # Async do |task|
    @max_workers.times do |i|
      @workers << Async do
        worker_loop(i)
      end
    end
    # end
  end

  def enqueue(job)
    @queue.enqueue(job)
  end

  def stop
    ''
  end

  def size
    @queue.size
  end

  private

  def worker_loop(worker_id)
    while @running
      begin
        Console.logger.info 'aqui!'
        job = @queue.dequeue
        # fallback_job = @fallback_queue.dequeue
        process_job(job, worker_id) # if job
        Console.logger.info("Worker #{worker_id} pegou job: #{job.inspect}") if job
        # process_fallback(fallback_job, worker_id) if fallback_job
        unless @fallback_queue.empty?
          fallback_job = @fallback_queue.dequeue
          process_fallback(fallback_job, worker_id) if fallback_job
          Console.logger.info("Worker #{worker_id} pegou fallback: #{fallback_job.inspect}") if fallback_job
        end
      rescue StandardError => e
        Console.logger.info "Error processing job in worker #{worker_id}: #{e.message}"
        Async::Task.current.sleep(1)
      end
    end
  end

  def process_job(job, worker_id)
    Console.logger.info("Iniciando job #{job}")
    unless @healthcheck&.healthy?
      Console.logger.info('Healthcheck ta ruim, job enfileirado novamente')
      @fallback_queue.enqueue(job)
      return
    end

    # Async do
    # endpoint = Async::HTTP::Endpoint.parse(DEFAULT_PROCESSOR_URL)
    # client = Async::HTTP.Client.new(endpoint, retries: 2)

    # response = client.post("/payments", [["content-type", "application/json"]], job[:data])
    # if response.ok?
    #   Console.logger.info("Job #{job} processado com sucesso pelo worker #{worker_id}")
    #   client&.close
    # end
    uri = URI.parse(DEFAULT_PROCESSOR_URL)
    http = Net::HTTP.new(uri.host, uri.port)
    http.max_retries = 2
    req = Net::HTTP::Post.new('/payments')
    req['Content-Type'] = 'application/json'
    req.body = job.to_json

    http.request(req) do |response|
      if response.is_a?(Net::HTTPSuccess)
        Console.logger.info("Job #{job} processado com sucesso pelo worker #{worker_id} na rota: #{uri}")
        @session.call("INSERT INTO payments (correlationId, amount, requested_at, p) VALUES('#{job[:correlationId]}', #{job[:amount]}, '#{job[:requestedAt]}', 'default')")
      else
        Console.logger.info("Erro ao processar job #{job} pelo worker #{worker_id}: #{response.message}")
        retry_job(job, 'fallback')
      end
    end
  rescue StandardError => e
    Console.logger.info("Error processing job #{job} in worker #{worker_id}: #{e.message}")
    retry_job(job, 'default')
    # ensure
    #  @session.close
    # end
  end

  def process_fallback(job, worker_id)
    # Async do
    uri = URI.parse(FALLBACK_PROCESSOR_URL)
    http = Net::HTTP.new(uri.host, uri.port)
    http.max_retries = 2
    req = Net::HTTP::Post.new('/payments')
    req['Content-Type'] = 'application/json'
    req.body = job.to_json

    http.request(req) do |response|
      if response.is_a?(Net::HTTPSuccess)
        Console.logger.info("Job #{job} processado com sucesso pelo worker #{worker_id}")
        @session.call("INSERT INTO payments (correlationId, amount, requested_at, p) VALUES('#{job[:correlationId]}', #{job[:amount]}, '#{job[:requestedAt]}', 'default')")
      else
        Console.logger.info("Erro ao processar job #{job} pelo worker #{worker_id}: #{response.message} no fallback")
        retry_job(job, 'fallback')
      end
    end
  rescue StandardError => e
    Console.logger.info("Error processing job #{job} in worker #{worker_id}: #{e.message}")
    retry_job(job, 'default')
    # ensure
    #  @session.close
    # end
  end

  def retry_job(job, processor)
    Async::Task.current.sleep(2)
    @fallback_queue.enqueue(job) if processor == 'fallback'
    @queue.enqueue(job) if processor == 'default'
  end
end

session = CLIENT.session
HEALTHCHECK = DefaultHealthcheck.new(check_interval: 5)
PAYMENT_QUEUE = AsyncPaymentQueue.new(max_workers: 10, healthcheck: HEALTHCHECK, session: session)

Async do
  Console.logger.info 'antes do start'
  PAYMENT_QUEUE.start
  Console.logger.info 'depois do start'
  Console.logger.info "#{PAYMENT_QUEUE.size} jobs enfileirados"

  Guaraci::Server.run(host: '0.0.0.0', port: 3000) do |request|
    case [request.method, request.path_segments]
    in ['POST', ['payments']]
      payment_data = {
        amount: request.params['amount'],
        correlationId: request.params['correlationId'],
        requestedAt: Time.now.iso8601
      }

      PAYMENT_QUEUE.enqueue(payment_data)
      Console.logger.info("Job enfileirado: #{PAYMENT_QUEUE.size}")
      Guaraci::Response.ok.render
    in ['GET', [rest]]
      handle_summary(rest)
    else
      Guaraci::Response.ok.json({ message: 'nada' }).render
    end
  end

  def handle_summary(path)
    params = path.split('?')[1].split('&')&.map { |q| q.split('=') }.to_a
    session = CLIENT.session
    unless params.empty?
      from = params[0][1]
      to = params[1][1]
      Console.logger.info("Agregando resumo dos pagamentos, de: #{from}, ate: #{to}")
      result = session.call("SELECT COUNT(*) as total_p, SUM(amount) as a, p as p FROM payments WHERE requested_at BETWEEN '#{from}' AND '#{to}' GROUP BY p")
      Console.logger.info("Resultado do resumo: #{result.to_a.inspect}")

      return Guaraci::Response.ok.json({
                                         default: {
                                           totalRequests: result.to_a[0][1],
                                           totalAmount: result.to_a[0][2]
                                         },
                                         fallback: {
                                           totalRequests: result.to_a[1][1],
                                           totalAmount: result.to_a[1][2]
                                         }
                                       }).render if result.to_a.any?
    end

    result = session.call("SELECT COUNT(*) as total_p, SUM(amount) as a, p as p FROM payments WHERE requested_at BETWEEN '#{from}' AND '#{to}' GROUP BY p")
    Console.logger.info("Resultado do resumo: #{result.to_a.inspect} fora do unless")
# "Resultado do resumo: [[366, 0.72834e4, \"default\"]]
## se nao tiver fallback no banco resolver
    Guaraci::Response.ok.json({
                                default: {
                                  totalRequests: result.to_a[0][0],
                                  totalAmount: result.to_a[0][1]
                                },
                                fallback: {
                                  totalRequests: result.to_a[1][0],
                                  totalAmount: result.to_a[1][1]
                                }
                              }).render if result.to_a.any?
  end
end
