export type OrderRecord = {
  partition: number;
  offset: number;
  key: string;
  value: string;
};

const ITEMS = ['book', 'lamp', 'pen', 'mug', 'desk', 'chair', 'plant'];
const STATUSES = ['placed', 'paid', 'packed', 'shipped'];
const FIRST_OFFSETS = [418, 395, 402];

export function order(sequence: number): OrderRecord {
  const id = 1040 + sequence;
  const partition = sequence % FIRST_OFFSETS.length;

  return {
    partition,
    offset: FIRST_OFFSETS[partition] + Math.floor(sequence / FIRST_OFFSETS.length),
    key: `order-${id}`,
    value: JSON.stringify({
      id,
      item: ITEMS[(id * 5) % ITEMS.length],
      status: STATUSES[(id * 3) % STATUSES.length],
    }),
  };
}
