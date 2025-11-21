module.exports = {
  clientId: 'order-system',
  brokers: ['localhost:9092'],
  topics: {
    orders: 'orders',
    ordersRetry: 'orders-retry',
    ordersDLQ: 'orders-dlq'
  }
};