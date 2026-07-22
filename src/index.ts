import { Elysia, sse, t } from 'elysia'
import { staticPlugin } from '@elysia/static'
import * as Longbridge from 'longbridge'

const config = Longbridge.Config.fromApikeyEnv()
const Tradectx = Longbridge.TradeContext.new(config)
const Quotectx = Longbridge.QuoteContext.new(config)
const Calendarctx = Longbridge.CalendarContext.new(config)
const Contentctx = Longbridge.ContentContext.new(config)
const Screenerctx = Longbridge.ScreenerContext.new(config)
const Fundamentalctx = Longbridge.FundamentalContext.new(config)


Quotectx.setOnQuote((err, event) => {
  if (err) {
    console.error("行情出错:", err);
    return;
  }
  const d = {
    name: String(event.symbol),
    data: event.data
  };
  channel.send(String('quote'), JSON.stringify(d));
});

Quotectx.setOnCandlestick((err, event) => {
  if (err) {
    console.error("行情出错:", err);
    return;
  }

  const B = event.data

  const data = {
    name: String(event.symbol),
    barsize: Number(B.period),
    bar: {
      time: new Date(B.candlestick.timestamp).getTime(),
      open: Number(B.candlestick.open),
      high: Number(B.candlestick.high),
      low: Number(B.candlestick.low),
      close: Number(B.candlestick.close),
      volume: Number(B.candlestick.volume),
    }
  }
  channel.send(String('bar'), JSON.stringify(data));
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

const cache = new Map<string, any>();
const queue: (() => void)[] = [];
let processing = false;

const processQueue = () => {
  if (processing || queue.length === 0) return;
  processing = true;

  const task = queue.shift();
  task?.();

  setTimeout(() => {
    processing = false;
    processQueue();
  }, 1000);
};

const app = new Elysia()
  .use(staticPlugin({ assets: 'public', prefix: '/' }))
  .listen(3000)
  .get('/sse', async function* () {

    yield sse({ event: "system", data: { message: "系统已连接" } })
    for await (const { event, data } of channel) {
      yield sse({ event, data })
    }
  })


  .get("/quotes", ({ query }) => {
    const rawSymbols = query.symbols || "";
    const symbols = rawSymbols
      .split(",")
      .map(s => s.trim().toUpperCase())
      .filter(s => s.length > 0);
    return Quotectx.quote(symbols);
  }, {
    query: t.Object({
      symbols: t.Optional(t.String())
    })
  })
  .get("/bars", async ({ query: { symbol, period = 14, adjustType = 1, start, end, sessions = 0 } }) => {
    const toNaiveDate = (timestamp: any) => {
      if (!timestamp) return null;
      const date = new Date(Number(timestamp) * 1000);
      if (isNaN(date.getTime())) return null;

      return new Longbridge.NaiveDate(
        date.getFullYear(),
        date.getMonth() + 1,
        date.getDate()
      );
    };

    const startDate = toNaiveDate(start);
    const endDate = toNaiveDate(end);

    return await Quotectx.historyCandlesticksByDate(
      (symbol).toUpperCase() as string,
      Number(period) as Longbridge.Period,
      Number(adjustType) as Longbridge.AdjustType,
      startDate as any,
      endDate as any,
      Number(sessions) as any //0盘中,1全部
    )
  })

  .get('/quote/:action/:symbol', async ({ params: { action, symbol } }) => {
    const symbols = symbol.split(',').map(s => s.trim().toUpperCase());
    if (action) {
      await Quotectx.subscribe(symbols, [0]);
    } else {
      await Quotectx.unsubscribe(symbols, [0]);
    }
    return await Quotectx.subscriptions();
  }, {
    params: t.Object({
      action: t.Boolean(),
      symbol: t.String()
    })
  })

  .get('/bar/:action/:symbol/:period', async ({ params: { action, symbol, period } }) => {
    if (action) {
      await Quotectx.subscribeCandlesticks(symbol.toUpperCase(), period, 1);
    } else {
      await Quotectx.unsubscribeCandlesticks(symbol.toUpperCase(), period);
    }
    return await Quotectx.subscriptions();
  }, {
    params: t.Object({
      action: t.Boolean(),
      symbol: t.String(),
      period: t.Number()
    })
  })

  .get("/info/:name", async ({ params, set }) => {
    const symbol = params.name;

    if (cache.has(symbol)) {
      return cache.get(symbol);
    }

    return new Promise((resolve, reject) => {
      queue.push(async () => {
        try {

          const data = await Fundamentalctx.company(symbol);
          const company = Array.isArray(data) ? data[0] : data;
          if (company && typeof company === 'object' && 'ticker' in company) {
            cache.set(symbol, data);
            resolve(data);
          } else {
            resolve({ error: "Symbol not found" });
          }
        } catch (err) {
          reject(err);
        }
      });

      processQueue();
    });
  })
  .get("/news", ({ query: { symbol } }) => {
    return Contentctx.news((symbol).toUpperCase());
  }, {
    query: t.Object({
      symbol: t.String()
    })
  })
  .get("/watchlists", () => {
    return Quotectx.watchlist();
  })
  .get("/watchlist/new", async ({ query: { name, symbols } }) => {

    const securities = symbols?.trim()
      ? symbols.split(',').map(s => s.trim().toUpperCase())
      : undefined;


    return await Quotectx.createWatchlistGroup({ name, securities });
  }, {
    query: t.Object({
      name: t.String(),
      symbols: t.Optional(t.String())
    })
  })
  .get("/watchlist/del", ({ query: { id } }) => {
    return Quotectx.deleteWatchlistGroup({ id, purge: true });
  }, {
    query: t.Object({
      id: t.Number()
    })
  })

  .get("/watchlist/edit", ({ query: { action, id, name, symbols } }) => {
    const list = symbols?.trim()
      ? symbols.split(',').map(s => s.trim().toUpperCase())
      : undefined;

    return Quotectx.updateWatchlistGroup({ id: Number(id), name: String(name), securities: list, mode: Number(action) });
  }, {
    query: t.Object({
      action: t.Number(),
      id: t.Number(),
      name: t.Optional(t.String()),
      symbols: t.Optional(t.String()),
    })
  })

console.log(
  `🦊 Elysia is running at ${app.server?.hostname}:${app.server?.port}`
);