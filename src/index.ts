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
  const WAITING_QUEUE = "w";
  const PROCESSED_SET = "p";
  const PROCESSOR: string[] = ["default", "fallback"];
  const STATUS: string[] = ["inactive", "active"];
  const STATUS_SET: Record<string, string> = { default: "ds", fallback: "fs" };
  await redis.set(STATUS_SET[PROCESSOR[0]], STATUS[1], "NX");
  await redis.set(STATUS_SET[PROCESSOR[1]], STATUS[1], "NX");

  const IS_PROCESSING_SET: Record<string, string> = { default: "ipd", fallback: "ipf" };
  await redis.set(IS_PROCESSING_SET[PROCESSOR[0]], STATUS[0], "NX");
  await redis.set(IS_PROCESSING_SET[PROCESSOR[1]], STATUS[0], "NX");

  checkProcessor(PROCESSOR[0]);
  await new Promise((resolve) => setTimeout(resolve, 2525));
  checkProcessor(PROCESSOR[1]);

  app.post("/payments", async (c) => {
    const body = await c.req.json<Payment>();
    const defaultStatus = await redis.get(STATUS_SET[PROCESSOR[0]]);
    if (defaultStatus === STATUS[1]) {
      const res = await processPayment(PROCESSOR[0], body);
      if (res.ok || res.status === 409) {
        return res;
      }
    }
    // const fallbackStatus = await redis.get(STATUS_SET[PROCESSOR[1]]);
    // if (fallbackStatus === STATUS[1]) {
    //   const res = await processPayment(PROCESSOR[1], body);
    //   if (res.ok || res.status === 409) {
    //     return res;
    //   }
    // }
    await redis.lpush(WAITING_QUEUE, `${body.correlationId}:${body.amount}`);
    return c.json("Queued", 202);
  });

  async function processPayment(processor: string, payment: Payment) {
    const dedupResult = await redis.dedupCheck(PROCESSED_SET, payment.correlationId);
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
        `${processor}`,
        currentDate.getTime(),
        `${payment.correlationId}:${payment.amount}`
      );
    } else {
      await redis.set(STATUS_SET[processor], STATUS[0]);
      await redis.srem(PROCESSED_SET, payment.correlationId);
    }
    return response;
  }

  async function checkProcessor(processor: string) {
    setInterval(async () => {
      const res = await fetch(`http://payment-processor-${processor}:8080/payments/service-health`);
      if (res.status >= 400) {
        return;
      }
      const body = await res.json();
      console.log(`${processor}  ${body.failing}`);
      if (body.failing || body.minResponseTime > 100) {
        await redis.set(STATUS_SET[processor], STATUS[0]);
      } else {
        await redis.set(STATUS_SET[processor], STATUS[1]);
        const queueLen = await redis.llen(WAITING_QUEUE);
        console.log(`queue len ${queueLen}`);
        if (queueLen !== 0) {
          let setExists = await redis.exists(IS_PROCESSING_SET[processor]);
          if (!setExists) {
            await redis.set(IS_PROCESSING_SET[processor], STATUS[0]);
          }
          let isProcessing = await redis.get(IS_PROCESSING_SET[processor]);
          console.log(`is processing ${processor} ${isProcessing}`);
          if (isProcessing === STATUS[0]) {
            await redis.set(IS_PROCESSING_SET[processor], STATUS[1]);
            processQueuePayments(processor, "right");
            processQueuePayments(processor, "left");
          }
        }
      }
    }, 5050);
  }

  async function processQueuePayments(processor: string, popFrom: string) {
    console.log(`processing queue payments ${processor} ${popFrom}`);
    let pop: (key: string) => Promise<string | null>;
    if (popFrom === "left") {
      pop = redis.lpop.bind(redis);
    } else {
      pop = redis.rpop.bind(redis);
    }
    while (true) {
      const item = await pop(WAITING_QUEUE);
      // console.log(processor);
      if (!item) {
        await redis.set(IS_PROCESSING_SET[processor], STATUS[0]);
        return;
      }
      const [_correlationId, _amount] = item!.split(":");
      const payment: Payment = {
        correlationId: _correlationId,
        amount: Number(_amount),
      };
      const res = await processQueuePayment(processor, payment);
      if (!res.ok) {
        await redis.set(IS_PROCESSING_SET[processor], STATUS[0]);
        return;
      }
    }
  }

  async function processQueuePayment(processor: string, payment: Payment) {
    const dedupResult = await redis.dedupCheck(PROCESSED_SET, payment.correlationId);
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
        `${processor}`,
        currentDate.getTime(),
        `${payment.correlationId}:${payment.amount}`
      );
    } else {
      await redis.set(STATUS_SET[processor], STATUS[0]);
      await redis.srem(PROCESSED_SET, payment.correlationId);
      await redis.rpush(WAITING_QUEUE, `${payment.correlationId}:${payment.amount}`);
    }
    return response;
  }

  app.get("/payments-summary", async (c) => {
    const from = c.req.query("from");
    const to = c.req.query("to");
    if (!from || !to) return c.text("Missing from or to query parameters", 400);
    const fromTimestamp = new Date(from).getTime();
    const toTimestamp = new Date(to).getTime();
    if (isNaN(fromTimestamp) || isNaN(toTimestamp)) return c.text("Invalid date format", 400);
    const defaultPayments = await redis.zrangebyscore(PROCESSOR[0], fromTimestamp, toTimestamp);
    const totalRequestsDefault = defaultPayments.length;
    const totalAmountDefault = defaultPayments.reduce(
      (sum: Decimal, val: string) => sum.plus(Decimal(val.split(":")[1])),
      new Decimal(0)
    );
    const fallbackPayments = await redis.zrangebyscore(PROCESSOR[1], fromTimestamp, toTimestamp);
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

  app.get("/status", async () => {
    const defaultResponse = await fetch(
      "http://payment-processor-default:8080/payments/service-health"
    );
    const fallbackResponse = await fetch(
      "http://payment-processor-fallback:8080/payments/service-health"
    );
    const defaultBody = await defaultResponse.json();
    const fallbackBody = await fallbackResponse.json();
    return new Response(JSON.stringify({ defaultBody, fallbackBody }), {
      status: defaultResponse.status,
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

  serve({ fetch: app.fetch, port: 3000, hostname: "0.0.0.0" });
  console.log("Hono server running at http://localhost:9999");
}

main().catch((err) => {
  console.error("App failed to start:", err);
});
