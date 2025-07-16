import { Hono } from "hono";
import { serve } from "@hono/node-server";
import Redis, { Redis as RedisClient } from "ioredis";
import { Decimal } from "decimal.js";

type Payment = {
  correlationId: string;
  amount: number;
};

async function main() {
  interface CustomRedis extends RedisClient {
    dedupCheck: (key: string, value: string) => Promise<number>;
  }
  const redis = new Redis("redis://redis:6379") as CustomRedis;
  redis.defineCommand("dedupCheck", {
    numberOfKeys: 1,
    lua: `
      if redis.call("SISMEMBER", KEYS[1], ARGV[1]) == 1 then
          return 0
      else
          redis.call("SADD", KEYS[1], ARGV[1])
          return 1
      end
    `,
  });
  const app = new Hono();
  await redis.lpush("waiting", "initializing");
  await redis.rpop("waiting");

  const PROCESSOR: string[] = ["default", "fallback"];
  const processorStatus: Record<string, boolean> = {
    default: false,
    fallback: false,
  };

  checkProcessor(PROCESSOR[0]);
  await new Promise((resolve) => setTimeout(resolve, 2525));
  checkProcessor(PROCESSOR[1]);

  async function processPayment(processor: string, payment: Payment) {
    const dedupResult = await redis.dedupCheck("payments:processed", payment.correlationId);
    if (dedupResult === 0) {
      return new Response("Duplicate payment", { status: 409 });
    }
    const currentDate = new Date();
    const response = await fetch(`http://payment-processor-${processor}:8080/payments`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        correlationId: payment.correlationId,
        amount: payment.amount,
        requestedAt: currentDate.toISOString(),
      }),
    });
    if (response.ok) {
      await redis.zadd(
        `payment:${processor}`,
        currentDate.getTime(),
        `${payment.correlationId}:${payment.amount}`
      );
    }
    return response;
  }

  app.post("/payments", async (c) => {
    const body = await c.req.json<Payment>();
    if (processorStatus[PROCESSOR[0]]) {
      const res = await processPayment(PROCESSOR[0], body);
      if (res.ok) {
        return res;
      }
    }
    if (processorStatus[PROCESSOR[1]]) {
      const res = await processPayment(PROCESSOR[1], body);
      if (res.ok) {
        return res;
      }
    }
    processorStatus[PROCESSOR[0]] = false;
    processorStatus[PROCESSOR[1]] = false;
    await redis.lpush("waiting", `${body.correlationId}:${body.amount}`);
  });

  async function checkProcessor(processor: string) {
    setInterval(async () => {
      const res = await fetch(`http://payment-processor-${processor}:8080/payments/service-health`);
      const body = await res.json();
      if (body.failure === true) {
        processorStatus[processor] = false;
      } else {
        processorStatus[processor] = true;
        processQueuePayments(processor);
      }
    }, 5050);
  }

  const isProcessingQueue: Record<string, boolean> = {};
  async function processQueuePayments(processor: string) {
    if (isProcessingQueue[processor]) return;
    isProcessingQueue[processor] = true;
    while (processorStatus[processor]) {
      const item = await redis.rpop("waiting");
      if (!item) {
        isProcessingQueue[processor] = false;
        return;
      }
      const [_correlationId, _amount] = item!.split(":");
      const payment: Payment = {
        correlationId: _correlationId,
        amount: Number(_amount),
      };
      await processPayment(processor, payment);
    }
    isProcessingQueue[processor] = false;
  }

  app.get("/payments-summary", async (c) => {
    const from = c.req.query("from");
    const to = c.req.query("to");
    if (!from || !to) return c.text("Missing from or to query parameters", 400);
    const fromTimestamp = new Date(from).getTime();
    const toTimestamp = new Date(to).getTime();
    if (isNaN(fromTimestamp) || isNaN(toTimestamp)) return c.text("Invalid date format", 400);
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
    return c.json({ default: defaultBody, fallback: fallbackBody }, { status: 200 });
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
