import { Elysia, sse, t } from 'elysia'
import { staticPlugin } from '@elysia/static'
import { cors } from '@elysia/cors'
import { openapi, fromTypes } from '@elysia/openapi'
import * as Longbridge from 'longbridge'
import { NaiveDatetime, NaiveDate, Time } from 'longbridge';
const config = Longbridge.Config.fromApikeyEnv()
const Tradectx = Longbridge.TradeContext.new(config)
const Quotectx = Longbridge.QuoteContext.new(config)

//quote订阅
Quotectx.setOnQuote((err, event) => {
  if (err) {
    console.error("行情出错:", err);
    return;
  }
  const q = event.data
  const S = String(event.symbol)
  const tv = {
    s: 'ok',
    n: `${S}`,
    v: {
      lp: Number(q.lastDone),
      open_price: Number(q.open),
      high_price: Number(q.high),
      low_price: Number(q.low),
      volume: Number(q.volume),
    }
  }
  channel.send(String('quote'), JSON.stringify(tv));
});

//k线订阅
Quotectx.setOnCandlestick((err, event) => {
  if (err) {
    console.error("行情出错:", err);
    return;
  }
  const B = event.data
  const S = String(event.symbol)
  const data = {
    name: S,
    barsize: B.period,
    bar: {
      time: Math.floor(new Date().getTime()),
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

const app = new Elysia()
  .use(openapi({
    references: fromTypes()
  }))
  .use(staticPlugin({ assets: 'public', prefix: '/' }))
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
  // .get("/", () => "Hello Elysia")

  // .get("/balance", async () => {
  //   return await Tradectx.accountBalance()
  // })
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
  .get('/history', async ({ query: { symbol, period, trade_session, from, to, count } }) => {
    return await Quotectx.historyCandlesticksByOffset(symbol, period, 1, false, fromSecondsToNaiveDatetime(to), count, trade_session);
  }, {
    query: t.Object({
      symbol: t.String(), period: t.Number(), trade_session: t.Number(), from: t.Number(), to: t.Number(), count: t.Number()//0盘中，100所有
    })
  })

  .get('/quote/:action/:symbol', async ({ params: { action, symbol } }) => {
    const q = symbol.toUpperCase();

    if (action) {
      await manager.add(q, 'quote', async () => {
        await Quotectx.subscribe([q], [0]);
      });
    } else {
      await manager.remove(q, async () => {
        await Quotectx.unsubscribe([q], [0]);
      });
    }
    return await Quotectx.subscriptions();
  }, {
    params: t.Object({
      action: t.Boolean(),
      symbol: t.String()
    })
  })

  .get('/bar/:action/:symbol/:period', async ({ params: { action, symbol, period } }) => {
    const q = `${symbol.toUpperCase()}_${period}`; // 唯一标识

    if (action) {
      await manager.add(q, 'bar', async () => {
        await Quotectx.subscribeCandlesticks(symbol.toUpperCase(), period, 1);
      });
    } else {
      await manager.remove(q, async () => {
        await Quotectx.unsubscribeCandlesticks(symbol.toUpperCase(), period);
      });
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


/**
 * 将秒级时间戳转换为 NaiveDatetime 对象
 * @param {number} timestampSeconds - 秒级时间戳
 * @returns {NaiveDatetime}
 */
function fromSecondsToNaiveDatetime(timestampSeconds: any) {
  // 1. 转为 JS Date 对象 (JS Date 需要毫秒，所以 * 1000)
  const date = new Date(timestampSeconds * 1000);

  // 2. 提取各项数值
  const year = date.getFullYear();
  const month = date.getMonth() + 1; // JS 月份从 0 开始，需要 +1
  const day = date.getDate();
  const hour = date.getHours();
  const minute = date.getMinutes();
  const second = date.getSeconds();

  // 3. 实例化对应的组件对象
  // 注意：这里的构造函数名称需根据你的 SDK 实际类名调整
  const naiveDate = new NaiveDate(year, month, day);
  const time = new Time(hour, minute, second);

  // 4. 返回 NaiveDatetime 实例
  return new NaiveDatetime(naiveDate, time);
}

class SubscriptionManager {
  private subscriptionMap = new Map<string, string>(); // Key: symbol, Value: type(quote/bar)
  private readonly MAX_LIMIT = 500;
  private readonly THRESHOLD = 10;

  async add(symbol: string, type: string, subscribeFn: () => Promise<void>) {
    // 1. 如果已存在，先移除（为了更新顺序）
    if (this.subscriptionMap.has(symbol)) {
      this.subscriptionMap.delete(symbol);
    }
    // 2. 检查额度：如果可用额度 < 10 (即已用 > 490)
    else if (this.subscriptionMap.size >= (this.MAX_LIMIT - this.THRESHOLD)) {
      const oldestSymbol = this.subscriptionMap.keys().next().value;
      if (oldestSymbol) {
        console.log(`额度紧张，自动退订最旧标的: ${oldestSymbol}`);
        // 这里需要根据实际业务逻辑调用对应的退订方法
        await this.remove(oldestSymbol);
      }
    }

    // 3. 执行订阅
    await subscribeFn();
    this.subscriptionMap.set(symbol, type);
  }

  async remove(symbol: string, unsubscribeFn?: () => Promise<void>) {
    if (this.subscriptionMap.has(symbol)) {
      if (unsubscribeFn) await unsubscribeFn();
      this.subscriptionMap.delete(symbol);
    }
  }
}

const manager = new SubscriptionManager();