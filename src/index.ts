import { Hono } from "hono";
import { serve } from "@hono/node-server";
import Redis from "ioredis";
import { Decimal } from "decimal.js";

type Payment = {
  correlationId: string;
  amount: number;
};

async function main() {
  const redis = new Redis("redis://redis:6379");
  const app = new Hono();
  await redis.lpush("waiting", "initializing");
  await redis.rpop("waiting");

  async function processPayment(processor: string, payment: Payment) {
    const currentDate = new Date();
    const response = await fetch(
      `http://payment-processor-${processor}:8080/payments`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          correlationId: payment.correlationId,
          amount: payment.amount,
          requestedAt: currentDate.toISOString(),
        }),
      }
    );
    if (response.ok) {
      await redis.zadd(
        `payment:${processor}`,
        currentDate.getTime(),
        `${payment.correlationId}:${payment.amount}`
      );
    }
    return response;
  }
  const PROCESSORS = ["default", "fallback"];
  let interval: any = undefined;
  app.post("/payments", async (c) => {
    const body = await c.req.json<Payment>();
    console.log(await redis.llen("waiting"))
    if (await redis.llen("waiting") === 0) {
      for (const processor of PROCESSORS) {
        const res = await processPayment(processor, body);
        if (res.ok) {
          return res;
        }
      }
    }
    await redis.lpush("waiting", `${body.correlationId}:${body.amount}`);
    if (interval === undefined) {
      processQueuePayments();
    }
  });

  async function processQueuePayments() {
    interval = setInterval(async () => {
      const item = await redis.rpop("waiting");
      if (!item) {
        clearInterval(interval);
        interval = undefined;
        return;
      }
      const [_correlationId, _amount] = item.split(':');
      const payment: Payment = {
        correlationId: _correlationId,
        amount: Number(_amount),
      };
      for (const processor of PROCESSORS) {
        const res = await processPayment(processor, payment);
        if (res.ok) {
          return res;
        } 
      }
      await redis.rpush(
        "waiting",
        `${payment.correlationId}:${payment.amount}`
      );
    }, 15);
  }

  app.get("/payments-summary", async (c) => {
    const from = c.req.query("from");
    const to = c.req.query("to");
    if (!from || !to) return c.text("Missing from or to query parameters", 400);
    const fromTimestamp = new Date(from).getTime();
    const toTimestamp = new Date(to).getTime();
    if (isNaN(fromTimestamp) || isNaN(toTimestamp))
      return c.text("Invalid date format", 400);
    const defaultPayments = await redis.zrangebyscore(
      "payment:default",
      fromTimestamp,
      toTimestamp
    );
    const totalRequestsDefault = defaultPayments.length;
    const totalAmountDefault = defaultPayments.reduce(
      (sum: Decimal, val: string) => sum.plus(Decimal(val.split(":")[1])),
      new Decimal(0)
    );
    const fallbackPayments = await redis.zrangebyscore(
      "payment:fallback",
      fromTimestamp,
      toTimestamp
    );
    const totalRequestsFallback = fallbackPayments.length;
    const totalAmountFallback = fallbackPayments.reduce(
      (sum: Decimal, val: string) => sum.plus(Decimal(val.split(":")[1])),
      new Decimal(0)
    );
    return c.json({
      default: {
        totalRequests: totalRequestsDefault,
        totalAmount: Number(totalAmountDefault.toFixed(2)),
      },
      fallback: {
        totalRequests: totalRequestsFallback,
        totalAmount: Number(totalAmountFallback.toFixed(2)),
      },
    });
  });

  app.get("/admin/payments-summary", async (c) => {
    const from = c.req.query("from");
    const to = c.req.query("to");
    if (!from || !to) return c.text("Missing from or to query parameters", 400);
    const defaultResponse = await fetch(
      `http://payment-processor-default:8080/admin/payments-summary?from=${from}&to=${to}`,
      {
        method: "GET",
        headers: { "X-Rinha-Token": "123" },
      }
    );
    const fallbackResponse = await fetch(
      `http://payment-processor-fallback:8080/admin/payments-summary?from=${from}&to=${to}`,
      {
        method: "GET",
        headers: { "X-Rinha-Token": "123" },
      }
    );
    const defaultBody = await defaultResponse.json();
    const fallbackBody = await fallbackResponse.json();
    return c.json(
      { default: defaultBody, fallback: fallbackBody },
      { status: 200 }
    );
  });

  app.post("/purge-payments", async () => {
    await fetch("http://payment-processor-default:8080/admin/purge-payments", {
      method: "POST",
      headers: { "X-Rinha-Token": "123" },
    });
    await fetch("http://payment-processor-fallback:8080/admin/purge-payments", {
      method: "POST",
      headers: { "X-Rinha-Token": "123" },
    });
    await redis.flushdb();
    return new Response(null, { status: 200 });
  });

  app.put("/admin/configurations/failure", async (c) => {
    const rawBody = await c.req.text();
    const defaultResponse = await fetch(
      "http://payment-processor-default:8080/admin/configurations/failure",
      {
        method: "PUT",
        headers: { "Content-Type": "application/json", "X-Rinha-Token": "123" },
        body: rawBody,
      }
    );
    const fallbackResponse = await fetch(
      "http://payment-processor-fallback:8080/admin/configurations/failure",
      {
        method: "PUT",
        headers: { "Content-Type": "application/json", "X-Rinha-Token": "123" },
        body: rawBody,
      }
    );
    const defaultBody = await defaultResponse.json();
    const fallbackBody = await fallbackResponse.json();
    return new Response(JSON.stringify({ defaultBody, fallbackBody }), {
      status: defaultResponse.status,
    });
  });

  app.get("/status", async () => {
    const defaultResponse = await fetch(
      "http://payment-processor-default:8080/payments/service-health"
    );
    const fallbackResponse = await fetch(
      "http://payment-processor-default:8080/payments/service-health"
    );
    const defaultBody = await defaultResponse.json();
    const fallbackBody = await fallbackResponse.json();
    return new Response(JSON.stringify({ defaultBody, fallbackBody }), {
      status: defaultResponse.status,
    });
  });

  serve({ fetch: app.fetch, port: 3000, hostname: "0.0.0.0" });
  console.log("Hono server running at http://localhost:9999");
}

main().catch((err) => {
  console.error("App failed to start:", err);
});
