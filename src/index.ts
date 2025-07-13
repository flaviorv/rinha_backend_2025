import { Hono } from 'hono'
import { serve } from '@hono/node-server'
import Redis, { Redis as RedisClient } from 'ioredis'
import { Decimal } from 'decimal.js'

type Payment = {
  correlationId: string
  amount: number
}

async function main() {
  
  interface CustomRedis extends RedisClient {
    dedupCheck: (key: string, value: string) => Promise<number>
  }
  const redis = new Redis('redis://redis:6379') as CustomRedis
  redis.defineCommand('dedupCheck', {
    numberOfKeys: 1,
    lua: `
      if redis.call("SISMEMBER", KEYS[1], ARGV[1]) == 1 then
          return 0
      else
          redis.call("SADD", KEYS[1], ARGV[1])
          return 1
      end
    `
  })

  const app = new Hono()

  app.post('/payments', async (c) => {
    const body = await c.req.json<Payment>()
    const currentDate = new Date()
    const dedupResult = await redis.dedupCheck('payments:processed', body.correlationId)
    if (dedupResult === 0) {
      return c.text('Duplicate payment', 409)
    }
    const response = await fetch('http://payment-processor-default:8080/payments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        correlationId: body.correlationId,
        amount: body.amount,
        requestedAt: currentDate.toISOString(),
      }),
    })
    if (!response.ok) {
      const fallbackResponse = await fetch('http://payment-processor-fallback:8080/payments', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          correlationId: body.correlationId,
          amount: body.amount,
          requestedAt: currentDate.toISOString(),
        })
      })
      if (!fallbackResponse.ok) {
          return c.text('Failed to fetch from external service', 502)
      }
      await redis.zadd('payment:fallback', currentDate.getTime(), `${body.amount}:${body.correlationId}`)
      return fallbackResponse
    }
    await redis.zadd('payment:default', currentDate.getTime(), `${body.amount}:${body.correlationId}`)    
    return response
  })

  app.put('/admin/configurations/failure', async(c) => {
    const response = fetch('http://payment-processor-default:8080/admin/configurations/failure', {
      method:'PUT',
      headers: {'Content-Type': 'application/json', 'X-Rinha-Token': '123'},
      body: JSON.stringify({failure: true})
    }) 
    return response
  })

  app.get('/payments-summary', async (c) => {
    const from = c.req.query('from')
    const to = c.req.query('to')
    if (!from || !to) return c.text('Missing from or to query parameters', 400)
    const fromTimestamp = new Date(from).getTime()
    const toTimestamp = new Date(to).getTime()
    if (isNaN(fromTimestamp) || isNaN(toTimestamp)) return c.text('Invalid date format', 400)
    const defaultPayments = await redis.zrangebyscore('payment:default', fromTimestamp, toTimestamp)
    const totalRequestsDefault = defaultPayments.length
    const totalAmountDefault = defaultPayments.reduce((sum: Decimal, val: string) => sum.plus(Decimal(val.split(':')[0])), new Decimal(0))
    const fallbackPayments = await redis.zrangebyscore('payment:fallback', fromTimestamp, toTimestamp)
    const totalRequestsFallback = fallbackPayments.length
    const totalAmountFallback = fallbackPayments.reduce((sum: Decimal, val: string) => sum.plus(Decimal(val.split(':')[0])), new Decimal(0))
    return c.json({
      default: {
        totalRequests: totalRequestsDefault,
        totalAmount: Number(totalAmountDefault.toFixed(2)),
      },
      fallback: {
        totalRequests: totalRequestsFallback,
        totalAmount: Number(totalAmountFallback.toFixed(2)),
      },
    })
  })

  app.get('/admin/payments-summary', async (c) => {
    const from = c.req.query('from')
    const to = c.req.query('to')
    if (!from || !to) return c.text('Missing from or to query parameters', 400)
    const defaultResponse = await fetch(`http://payment-processor-default:8080/admin/payments-summary?from=${from}&to=${to}`, {
      method: 'GET',
      headers: { 'X-Rinha-Token': '123' },
    })
    const fallbackResponse = await fetch(`http://payment-processor-fallback:8080/admin/payments-summary?from=${from}&to=${to}`, {
      method: 'GET',
      headers: { 'X-Rinha-Token': '123' },
    })
    const defaultBody = await defaultResponse.json()
    const fallbackBody = await fallbackResponse.json()
    return c.json({ default: defaultBody, fallback: fallbackBody }, { status: 200 })
  })

  app.post('/purge-payments', async () => {
    await fetch('http://payment-processor-default:8080/admin/purge-payments', {
      method: 'POST',
      headers: { 'X-Rinha-Token': '123' },
    })
    await fetch('http://payment-processor-fallback:8080/admin/purge-payments', {
      method: 'POST',
      headers: { 'X-Rinha-Token': '123' },
    })
    await redis.flushdb()
    return new Response(null, { status: 200 })
  })

  app.get('/status', async () => {
    return fetch('http://payment-processor-default:8080/payments/service-health')
  })
  serve({ fetch: app.fetch, port: 3000, hostname: '0.0.0.0' })
  console.log('Hono server running at http://localhost:3000')
}

main().catch((err) => {
  console.error('App failed to start:', err)
})
