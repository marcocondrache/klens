import { GENERATED_AT, Rand } from "./mock-data"
import type { Compression, RecordHeader, Topic, TopicRecord } from "./types"

const FIRST_NAMES = ["ada", "linus", "grace", "rob", "barbara", "ken", "margaret", "alan", "edsger", "leslie"]
const LAST_NAMES = ["lovelace", "torvalds", "hopper", "pike", "liskov", "thompson", "hamilton", "kay", "dijkstra", "lamport"]
const CITIES = ["Milan", "Berlin", "Lisbon", "Dublin", "Warsaw", "Amsterdam", "Zurich", "Paris", "Madrid", "Oslo"]
const COUNTRIES = ["IT", "DE", "PT", "IE", "PL", "NL", "CH", "FR", "ES", "NO"]
const CURRENCIES = ["EUR", "EUR", "EUR", "USD", "GBP", "CHF"]
const CARRIERS = ["dhl", "ups", "gls", "fedex", "poste"]
const DEVICES = ["ios", "android", "web-chrome", "web-safari", "web-firefox"]
const SERVICES = ["checkout-api", "order-service", "payment-gateway", "web-storefront", "mobile-bff"]
const COMPRESSIONS: Compression[] = ["NONE", "SNAPPY", "LZ4", "ZSTD", "GZIP"]

function sku(rand: Rand) {
  return `SKU-${rand.int(1000, 9999)}-${rand.pick(["S", "M", "L", "XL"])}`
}

function uuid(rand: Rand) {
  const hex = (length: number) =>
    Array.from({ length }, () => rand.int(0, 15).toString(16)).join("")
  return `${hex(8)}-${hex(4)}-4${hex(3)}-a${hex(3)}-${hex(12)}`
}

function money(rand: Rand, min: number, max: number) {
  return Number((min + rand.next() * (max - min)).toFixed(2))
}

function payloadFor(topic: string, rand: Rand, timestamp: number): { key: string | null; value: unknown } {
  const at = new Date(timestamp).toISOString()
  const family = topic.split(".")[0]

  if (topic.startsWith("__consumer_offsets")) {
    return {
      key: `[${rand.pick(["order-processor", "analytics-etl", "payment-service"])},orders.created,${rand.int(0, 11)}]`,
      value: {
        offset: rand.int(1_000_000, 9_000_000),
        leaderEpoch: rand.int(1, 40),
        metadata: "",
        commitTimestamp: timestamp,
      },
    }
  }

  switch (family) {
    case "orders": {
      const orderId = `ord_${uuid(rand).slice(0, 12)}`
      const items = Array.from({ length: rand.int(1, 4) }, () => ({
        sku: sku(rand),
        quantity: rand.int(1, 3),
        unitPrice: money(rand, 9.9, 249.9),
      }))

      return {
        key: orderId,
        value: {
          orderId,
          customerId: `cus_${uuid(rand).slice(0, 10)}`,
          status: topic.endsWith("cancelled")
            ? "CANCELLED"
            : rand.pick(["PLACED", "CONFIRMED", "PICKING", "AWAITING_PAYMENT"]),
          items,
          total: Number(items.reduce((sum, item) => sum + item.quantity * item.unitPrice, 0).toFixed(2)),
          currency: rand.pick(CURRENCIES),
          shippingCountry: rand.pick(COUNTRIES),
          createdAt: at,
        },
      }
    }

    case "payments": {
      const paymentId = `pay_${uuid(rand).slice(0, 12)}`

      return {
        key: paymentId,
        value: {
          paymentId,
          orderId: `ord_${uuid(rand).slice(0, 12)}`,
          amount: money(rand, 12, 890),
          currency: rand.pick(CURRENCIES),
          method: rand.pick(["card", "card", "sepa_debit", "paypal", "apple_pay"]),
          status: topic.endsWith("failed")
            ? "FAILED"
            : topic.endsWith("captured")
              ? "CAPTURED"
              : "AUTHORIZED",
          processor: rand.pick(["stripe", "adyen", "braintree"]),
          declineCode: topic.endsWith("failed") ? rand.pick(["insufficient_funds", "do_not_honor", "expired_card"]) : null,
          processedAt: at,
        },
      }
    }

    case "inventory": {
      const itemSku = sku(rand)

      return {
        key: itemSku,
        value: {
          sku: itemSku,
          warehouse: rand.pick(["wh-milan", "wh-berlin", "wh-lisbon"]),
          available: rand.int(0, 480),
          reserved: rand.int(0, 60),
          reservedFor: topic.endsWith("reserved") ? `ord_${uuid(rand).slice(0, 12)}` : null,
          updatedAt: at,
        },
      }
    }

    case "shipments": {
      const shipmentId = `shp_${uuid(rand).slice(0, 12)}`

      return {
        key: shipmentId,
        value: {
          shipmentId,
          orderId: `ord_${uuid(rand).slice(0, 12)}`,
          carrier: rand.pick(CARRIERS),
          trackingNumber: `${rand.pick(CARRIERS).toUpperCase()}${rand.int(10_000_000, 99_999_999)}`,
          status: topic.endsWith("delivered") ? "DELIVERED" : "IN_TRANSIT",
          destination: { city: rand.pick(CITIES), country: rand.pick(COUNTRIES) },
          dispatchedAt: at,
        },
      }
    }

    case "users": {
      const first = rand.pick(FIRST_NAMES)
      const last = rand.pick(LAST_NAMES)
      const userId = `usr_${uuid(rand).slice(0, 10)}`

      return {
        key: userId,
        value: {
          userId,
          email: `${first}.${last}@example.com`,
          name: `${first} ${last}`,
          country: rand.pick(COUNTRIES),
          plan: rand.pick(["free", "plus", "pro"]),
          marketingOptIn: rand.chance(0.35),
          updatedAt: at,
        },
      }
    }

    case "notifications": {
      const userId = `usr_${uuid(rand).slice(0, 10)}`

      return {
        key: userId,
        value: {
          notificationId: `ntf_${uuid(rand).slice(0, 12)}`,
          userId,
          channel: topic.endsWith("push") ? "push" : "email",
          template: rand.pick(["order_confirmation", "shipment_update", "password_reset", "cart_reminder"]),
          locale: rand.pick(["it-IT", "de-DE", "en-GB", "fr-FR"]),
          status: rand.pick(["QUEUED", "SENT", "SENT", "BOUNCED"]),
          sentAt: at,
        },
      }
    }

    case "analytics": {
      const sessionId = uuid(rand)

      return {
        key: sessionId,
        value: {
          sessionId,
          userId: rand.chance(0.7) ? `usr_${uuid(rand).slice(0, 10)}` : null,
          event: topic.endsWith("clicks") ? "click" : "pageview",
          path: rand.pick(["/", "/search", "/product/1042", "/cart", "/checkout", "/account/orders"]),
          referrer: rand.pick(["https://google.com", "direct", "https://instagram.com", "https://news.ycombinator.com"]),
          device: rand.pick(DEVICES),
          durationMs: rand.int(120, 48_000),
          at,
        },
      }
    }

    case "search": {
      return {
        key: uuid(rand).slice(0, 8),
        value: {
          queryId: uuid(rand),
          term: rand.pick(["running shoes", "wool coat", "espresso machine", "usb-c hub", "desk lamp", "linen shirt"]),
          filters: { category: rand.pick(["apparel", "home", "electronics"]), maxPrice: rand.int(50, 500) },
          results: rand.int(0, 840),
          latencyMs: rand.int(4, 210),
          at,
        },
      }
    }

    case "cart": {
      const cartId = `crt_${uuid(rand).slice(0, 10)}`

      return {
        key: cartId,
        value: {
          cartId,
          userId: `usr_${uuid(rand).slice(0, 10)}`,
          action: rand.pick(["item_added", "item_removed", "quantity_changed", "cart_viewed", "checkout_started"]),
          sku: sku(rand),
          quantity: rand.int(1, 4),
          cartValue: money(rand, 15, 620),
          at,
        },
      }
    }

    case "pricing": {
      const itemSku = sku(rand)
      const oldPrice = money(rand, 10, 300)

      return {
        key: itemSku,
        value: {
          sku: itemSku,
          oldPrice,
          newPrice: Number((oldPrice * (0.7 + rand.next() * 0.5)).toFixed(2)),
          currency: rand.pick(CURRENCIES),
          reason: rand.pick(["promotion", "cost_change", "competitor_match", "clearance"]),
          effectiveAt: at,
        },
      }
    }

    case "fraud": {
      return {
        key: `ord_${uuid(rand).slice(0, 12)}`,
        value: {
          signalId: uuid(rand),
          score: Number(rand.next().toFixed(3)),
          rules: ["velocity_check", "geo_mismatch", "bin_blocklist"].slice(0, rand.int(1, 3)),
          decision: rand.pick(["ALLOW", "ALLOW", "REVIEW", "BLOCK"]),
          evaluatedAt: at,
        },
      }
    }

    case "audit": {
      return {
        key: `usr_${uuid(rand).slice(0, 10)}`,
        value: {
          actor: `${rand.pick(FIRST_NAMES)}@klens.dev`,
          action: rand.pick(["topic.create", "acl.update", "config.alter", "group.reset-offsets", "user.login"]),
          resource: rand.pick(["orders.created", "payments.authorized", "cluster", "order-processor"]),
          ip: `10.42.${rand.int(1, 24)}.${rand.int(2, 250)}`,
          userAgent: rand.pick(["klens/0.1.0", "kafka-cli/3.9.1", "Mozilla/5.0"]),
          at,
        },
      }
    }

    case "cdc": {
      const table = topic.split(".").at(-1) ?? "orders"
      const id = rand.int(10_000, 99_999)
      const after =
        table === "customers"
          ? { id, email: `${rand.pick(FIRST_NAMES)}@example.com`, city: rand.pick(CITIES), tier: rand.pick(["bronze", "silver", "gold"]) }
          : { id, total: money(rand, 20, 700), status: rand.pick(["placed", "shipped", "returned"]) }

      return {
        key: JSON.stringify({ id }),
        value: {
          before: rand.chance(0.4) ? { ...after, status: "pending" } : null,
          after,
          source: {
            version: "2.7.4.Final",
            connector: "postgresql",
            name: "cdc",
            ts_ms: timestamp - rand.int(4, 90),
            db: "commerce",
            schema: "public",
            table,
            lsn: rand.int(24_000_000, 25_000_000),
          },
          op: rand.pick(["c", "u", "u", "d"]),
          ts_ms: timestamp,
        },
      }
    }

    case "dead-letter": {
      return {
        key: `ord_${uuid(rand).slice(0, 12)}`,
        value: {
          originalTopic: "orders.created",
          originalPartition: rand.int(0, 11),
          originalOffset: rand.int(1_000_000, 9_000_000),
          error: rand.pick([
            "org.apache.kafka.common.errors.SerializationException: Unknown magic byte!",
            "java.lang.NullPointerException: customerId must not be null",
            "io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Subject not found",
          ]),
          retries: rand.int(1, 5),
          failedAt: at,
        },
      }
    }

    default:
      return {
        key: uuid(rand).slice(0, 12),
        value: { id: uuid(rand), topic, sequence: rand.int(1, 1_000_000), at },
      }
  }
}

function headersFor(topic: string, rand: Rand): RecordHeader[] {
  const headers: RecordHeader[] = [
    {
      key: "traceparent",
      value: `00-${Array.from({ length: 32 }, () => rand.int(0, 15).toString(16)).join("")}-${Array.from(
        { length: 16 },
        () => rand.int(0, 15).toString(16),
      ).join("")}-01`,
    },
    { key: "content-type", value: "application/json" },
    { key: "source-service", value: rand.pick(SERVICES) },
  ]

  if (rand.chance(0.4)) {
    headers.push({ key: "schema-id", value: String(rand.int(1000, 1080)) })
  }

  if (topic.startsWith("dead-letter")) {
    headers.push({ key: "dlq-attempt", value: String(rand.int(1, 5)) })
  }

  return headers
}

export function buildRecord(clusterName: string, topic: Topic, partition: number, offset: number): TopicRecord {
  const rand = new Rand(`${clusterName}:${topic.name}:${partition}:${offset}`)
  const target = topic.partitions.find((part) => part.id === partition) ?? topic.partitions[0]
  const perSecond = Math.max(0.05, topic.messagesPerSec / Math.max(1, topic.partitions.length))
  const timestamp = Math.round(GENERATED_AT - ((target.highWatermark - offset) / perSecond) * 1000)

  const { key, value } = payloadFor(topic.name, rand, timestamp)
  const body = JSON.stringify(value, null, 2)

  return {
    topic: topic.name,
    partition,
    offset,
    timestamp,
    key,
    value: body,
    headers: headersFor(topic.name, rand),
    sizeBytes: body.length + (key?.length ?? 0) + 42,
    compression: rand.pick(COMPRESSIONS),
  }
}
