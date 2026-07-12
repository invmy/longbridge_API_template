import { Elysia, sse, t } from 'elysia'
import { cors } from '@elysia/cors'
import { openapi, fromTypes } from '@elysia/openapi'
import * as Longbridge from 'longbridge'

const config = Longbridge.Config.fromApikey('', '', '')
const Tradectx = Longbridge.TradeContext.new(config)
const Quotectx = Longbridge.QuoteContext.new(config)

//quote订阅
Quotectx.setOnQuote((err, event) => {
  if (err) {
    console.error("行情出错:", err);
    return;
  }
  console.log("收到行情:", event.symbol, event);
  channel.send(String(event.symbol), JSON.stringify(event));
});

//k线订阅
Quotectx.setOnCandlestick((err, event) => {
  if (err) {
    console.error("行情出错:", err);
    return;
  }
  console.log("收到k线:", event.symbol, event);
  channel.send(String(event.symbol), JSON.stringify(event));
});


function createChannel() {
  let queue: Array<{ event: string; data: string }> = []
  let resolveNext: (() => void) | null = null
  return {
    send(event: string, data: string) {
      queue.push({ event, data })
      resolveNext?.()
    },
    async *[Symbol.asyncIterator]() {
      while (true) {
        while (queue.length === 0) {
          await new Promise<void>((r) => { resolveNext = r })
        }
        yield* queue.splice(0)
      }
    }
  }
}
export const channel = createChannel()

const app = new Elysia()
  .use(openapi({
    references: fromTypes()
  }))
  .use(cors())
  .listen(3000)
  .get('/sse', async function* ({ set }) {
    set.headers['Content-Type'] = 'text/event-stream';
    set.headers['Cache-Control'] = 'no-cache';
    set.headers['Connection'] = 'keep-alive';
    for await (const { event, data } of channel) {
      yield sse({ event, data })
    }
  })
  .get("/", () => "Hello Elysia")

  .get("/balance", async () => {
    return await Tradectx.accountBalance()
  })
  .get('/info/:q', async ({ params: { q } }) => {
    const symbol = q.toUpperCase();
    return await Quotectx.staticInfo([symbol]);
  }, {
    params: t.Object({
      q: t.String()
    })
  })
  .get('/bars', async ({ query: { symbol, period, trade_session } }) => {
    return await Quotectx.candlesticks(symbol, period, 1000, 1, trade_session);
  }, {
    query: t.Object({
      symbol: t.String(), period: t.Number(), trade_session: t.Number()//0盘中，100所有
    })
  })
  .get('/quote/:action/:symbol', async ({ params: { action, symbol } }) => {
    const q = symbol.toUpperCase();
    const shouldSubscribe = action === true;

    if (shouldSubscribe) {
      await Quotectx.subscribe([q], [1]);
    } else {
      await Quotectx.unsubscribe([q], [1]);
    }

    return await Quotectx.subscriptions();
  }, {
    params: t.Object({
      action: t.Boolean(),
      symbol: t.String()
    })
  })

  .get('/bar/:action/:symbol/:period', async ({ params: { action, symbol, period } }) => {
    const q = symbol.toUpperCase();
    const shouldSubscribe = action === true;

    if (shouldSubscribe) {
      await Quotectx.subscribeCandlesticks(q, period, 1);
    } else {
      await Quotectx.unsubscribeCandlesticks(q, period);
    }

    return await Quotectx.subscriptions();
  }, {
    params: t.Object({
      action: t.Boolean(),
      symbol: t.String(),
      period: t.Number()
    })
  })

//......
console.log(
  `🦊 Elysia is running at ${app.server?.hostname}:${app.server?.port}`
);
