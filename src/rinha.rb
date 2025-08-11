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
    Async do
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
    ensure
      @checking = false
    end
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
    Async do |task|
      @max_workers.times do |i|
        @workers << task.async do
          worker_loop(i)
        end
      end
    end
  end

  def enqueue(job)
    @queue.enqueue(job)
  end

  def stop
    ''
  end

  private

  def worker_loop(worker_id)
    while @running
      begin
        job = @queue.dequeue
        fallback_job = @fallback_queue.dequeue
        process_job(job, worker_id) if job
        process_fallback(fallback_job, worker_id) if fallback_job
      rescue StandardError => e
        puts "Error processing job in worker #{worker_id}: #{e.message}"
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

    Async do
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
          Console.logger.info("Job #{job} processado com sucesso pelo worker #{worker_id}")
          @session.call("INSERT INTO payments (correlationId, amount, requested_at, p) VALUES('#{job[:correlationId]}', #{job[:amount]}, '#{job[:requestedAt]}', 'default')")
        else
          Console.logger.info("Erro ao processar job #{job} pelo worker #{worker_id}: #{response.message}")
          retry_job(job, 'fallback')
        end
      end
    rescue StandardError => e
      Console.logger.error("Error processing job #{job} in worker #{worker_id}: #{e.message}")
      retry_job(job, 'default')
    ensure
      @session.close
    end
  end

  def process_fallback(job, worker_id)
    Async do
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
          Console.logger.info("Erro ao processar job #{job} pelo worker #{worker_id}: #{response.message}")
          retry_job(job, 'fallback')
        end
      end
    rescue StandardError => e
      Console.logger.error("Error processing job #{job} in worker #{worker_id}: #{e.message}")
      retry_job(job, 'default')
    ensure
      @session.close
    end
  end

  def retry_job(job, processor)
    Async::Task.current.sleep(2)
    @fallback_queue.enqueue(job) if processor == 'fallback'
    @queue.enqueue(job) if processor == 'default'
  end
end

session = CLIENT.session
HEALTHCHECK = DefaultHealthcheck.new(check_interval: 5)
payment_queue = AsyncPaymentQueue.new(max_workers: 10, healthcheck: HEALTHCHECK, session: session)

Async do
  payment_queue.start

  Guaraci::Server.run(host: '0.0.0.0', port: 3000) do |request|
    case [request.method, request.path_segments]
    in ['POST', ['payments']]
      payment_data = {
        amount: request.params['amount'],
        correlationId: request.params['correlationId'],
        requestedAt: Time.now.iso8601
      }

      payment_queue.enqueue(payment_data)
      Guaraci::Response.ok.render
    end
  end
end

# executor = proc do |job|
#   NET::HTTP.start("0.0.0.0", 3000) do |http|
#     http.post("/payments", job)
#   end
# end
