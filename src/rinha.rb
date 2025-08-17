# frozen_string_literal: true

require 'guaraci'
require 'async'
require 'db/client'
require 'db/postgres'
require 'async/job'
require 'async/http'
require 'net/http'
require 'bigdecimal'

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
    check_health if should_check?

    @last_status.nil? || @last_status
  end

  private

  def should_check?
    return true if @last_check_time.nil?
    return false if @checking

    Process.clock_gettime(Process::CLOCK_MONOTONIC) - @last_check_time > @check_interval
  end

  def check_health
    return if @checking

    @checking = true
    begin
      uri = URI.parse(@healthcheck_url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.get('/payments/service-health') do |response|
        @last_status = !JSON.parse(response)['failing']
        @last_check_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    rescue StandardError => e
      @last_status = false
      @last_check_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
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
      @workers << Sync do
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

  def size
    @queue.size
  end

  private

  def worker_loop(worker_id)
    while @running
      begin
        job = @queue.dequeue
        process_job(job, worker_id) # if job
        unless @fallback_queue.empty?
          fallback_job = @fallback_queue.dequeue
          process_fallback(fallback_job, worker_id) if fallback_job
        end
      rescue StandardError => e
        Async::Task.current.sleep(1)
      end
    end
  end

  def process_job(job, worker_id)
    unless @healthcheck&.healthy?
      @fallback_queue.enqueue(job)
      return
    end

    uri = URI.parse(DEFAULT_PROCESSOR_URL)
    http = Net::HTTP.new(uri.host, uri.port)
    req = Net::HTTP::Post.new('/payments')
    req['Content-Type'] = 'application/json'
    payment_data = {
      amount: job.params['amount'],
      correlationId: job.params['correlationId'],
      requestedAt: Time.now.iso8601
    }
    req.body = payment_data.to_json

    http.request(req) do |response|
      if response.is_a?(Net::HTTPSuccess)
        @session.call("INSERT INTO payments (correlationId, amount, requested_at, p) VALUES('#{payment_data[:correlationId]}', #{payment_data[:amount]}, '#{payment_data[:requestedAt]}', 'default')")
      else
        retry_job(job, 'fallback')
      end
    end
  rescue StandardError => e
    retry_job(job, 'fallback')
  end

  def process_fallback(job, worker_id)
    uri = URI.parse(FALLBACK_PROCESSOR_URL)
    http = Net::HTTP.new(uri.host, uri.port)
    req = Net::HTTP::Post.new('/payments')
    req['Content-Type'] = 'application/json'

    payment_data = {
      amount: job.params['amount'],
      correlationId: job.params['correlationId'],
      requestedAt: Time.now.iso8601
    }
    req.body = payment_data.to_json

    http.request(req) do |response|
      if response.is_a?(Net::HTTPSuccess)
        @session.call("INSERT INTO payments (correlationId, amount, requested_at, p) VALUES('#{payment_data[:correlationId]}', #{payment_data[:amount]}, '#{payment_data[:requestedAt]}', 'fallback')")
      else
        retry_job(job, 'fallback')
      end
    end
  rescue StandardError => e
    retry_job(job, 'fallback')
 end

  def retry_job(job, processor)
    #Async::Task.current.sleep(2)
    @fallback_queue.enqueue(job) if processor == 'fallback'
    @queue.enqueue(job) if processor == 'default'
  end
end

Sync do
session = CLIENT.session
HEALTHCHECK = DefaultHealthcheck.new(check_interval: 5)
PAYMENT_QUEUE = AsyncPaymentQueue.new(max_workers: 10, healthcheck: HEALTHCHECK, session: session)


  PAYMENT_QUEUE.start

  Guaraci::Server.run(host: '0.0.0.0', port: 3000) do |request|
    case [request.method, request.path_segments]
    in ['POST', ['payments']]
      payment_data = {
        amount: request.params['amount'],
        correlationId: request.params['correlationId'],
        requestedAt: Time.now.iso8601
      }

      PAYMENT_QUEUE.enqueue(request)
      Guaraci::Response.ok.render
    in ['GET', [rest]]
      handle_summary(rest)
    else
      Guaraci::Response.ok.json({ message: 'nada' }).render
    end
  end

  def handle_summary(path)
    params = path&.split('?')&.[](1)&.split('&')&.map { |q| q&.split('=') }.to_a
    session = CLIENT.session
    unless params.empty?
      from = params[0][1]
      to = params[1][1]
      result = session.call("SELECT COUNT(*) as total_p, SUM(amount) as a, p as p FROM payments WHERE requested_at BETWEEN '#{from}' AND '#{to}' GROUP BY p")

      payments = result.to_a
      if payments.size == 1
        Guaraci::Response.ok.json({
                                    default: {
                                      totalRequests: payments[0][0],
                                      totalAmount: (BigDecimal(payments[0][1].to_s) * 100.0)
                                    },
                                    fallback: {
                                      totalRequests: 0,
                                      totalAmount: 0
                                    }
                                  }).render
      elsif payments.size == 2
        Guaraci::Response.ok.json({
                                    default: {
                                      totalRequests: payments[0][0],
                                      totalAmount: (BigDecimal(payments[0][1].to_s) * 100.0)
                                    },
                                    fallback: {
                                      totalRequests: payments[1][0],
                                      totalAmount: (BigDecimal(payments[1][1].to_s) * 100.0)
                                    }
                                  }).render
      else
        Guaraci::Response.ok.json({
                                    default: {
                                      totalRequests: 0,
                                      totalAmount: 0
                                    },
                                    fallback: {
                                      totalRequests: 0,
                                      totalAmount: 0
                                    }
                                  }).render

      end
    end

    result = session.call('SELECT COUNT(*) as total_p, SUM(amount) as a, p as p FROM payments GROUP BY p')

    payments = result.to_a
    if payments.size == 1
      Guaraci::Response.ok.json({
                                  default: {
                                    totalRequests: payments[0][0],
                                    totalAmount: (BigDecimal(payments[0][1].to_s) * 100.0)
                                  },
                                  fallback: {
                                    totalRequests: 0,
                                    totalAmount: 0
                                  }
                                }).render
    elsif payments.size == 2

      Guaraci::Response.ok.json({
                                  default: {
                                    totalRequests: payments[0][0],
                                    totalAmount: (BigDecimal(payments[0][1].to_s) * 100.0)
                                  },
                                  fallback: {
                                    totalRequests: payments[1][0],
                                    totalAmount: (BigDecimal(payments[1][1].to_s) * 100.0)
                                  }
                                }).render
    else
      Guaraci::Response.ok.json({
                                  default: {
                                    totalRequests: 0,
                                    totalAmount: 0
                                  },
                                  fallback: {
                                    totalRequests: 0,
                                    totalAmount: 0
                                  }
                                }).render

    end
  end
end
