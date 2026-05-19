const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const db = require('./database');
const { Kafka } = require('kafkajs');
const path = require('path');

const PROTO_PATH = path.resolve(__dirname, '../proto/orders.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const ordersProto = grpc.loadPackageDefinition(packageDefinition).orders;

// Kafka Producer Setup
const kafka = new Kafka({ clientId: 'orders-service', brokers: ['localhost:9092'] });
const producer = kafka.producer();

const initKafka = async () => {
  try {
    await producer.connect();
    console.log('Orders Service connected to Kafka');
  } catch (err) {
    console.error('Error connecting to Kafka:', err);
  }
};

const server = new grpc.Server();

server.addService(ordersProto.OrderService.service, {
  createOrder: (call, callback) => {
    const order = call.request;
    const orderId = Math.random().toString(36).substring(7);
    const status = 'CREATED';

    db.run(
      "INSERT INTO orders (id, user_id, product_id, quantity, status) VALUES (?, ?, ?, ?, ?)",
      [orderId, order.user_id, order.product_id, order.quantity, status],
      async function (err) {
        if (err) {
          callback({ code: grpc.status.INTERNAL, details: 'Internal Server Error' });
        } else {
          // Publish Event to Kafka
          const event = {
            type: 'ORDER_CREATED',
            orderId: orderId,
            productId: order.product_id,
            userId: order.user_id,
            quantity: order.quantity
          };
          
          try {
            await producer.send({
              topic: 'order-events',
              messages: [{ value: JSON.stringify(event) }],
            });
            console.log(`Event ORDER_CREATED sent for order ${orderId}`);
          } catch (kafkaErr) {
            console.error('Failed to send Kafka event:', kafkaErr);
          }

          callback(null, { id: orderId, user_id: order.user_id, product_id: order.product_id, quantity: order.quantity, status });
        }
      }
    );
  },
  getOrder: (call, callback) => {
    db.get("SELECT * FROM orders WHERE id = ?", [call.request.id], (err, row) => {
      if (err) {
        callback({ code: grpc.status.INTERNAL, details: 'Internal Server Error' });
      } else if (row) {
        callback(null, row);
      } else {
        callback({ code: grpc.status.NOT_FOUND, details: 'Order not found' });
      }
    });
  },
  updateOrderStatus: (call, callback) => {
    const { id, status } = call.request;
    db.run(
      "UPDATE orders SET status = ? WHERE id = ?",
      [status, id],
      function (err) {
        if (err) {
          callback({ code: grpc.status.INTERNAL, details: 'Internal Server Error' });
        } else if (this.changes === 0) {
          callback({ code: grpc.status.NOT_FOUND, details: 'Order not found' });
        } else {
          db.get("SELECT * FROM orders WHERE id = ?", [id], (err, row) => {
            callback(null, row);
          });
        }
      }
    );
  },
  deleteOrder: (call, callback) => {
    const orderId = call.request.id;
    db.get("SELECT * FROM orders WHERE id = ?", [orderId], (err, row) => {
      if (err) {
        callback({ code: grpc.status.INTERNAL, details: 'Internal Server Error' });
      } else if (!row) {
        callback(null, { success: false });
      } else {
        db.run("DELETE FROM orders WHERE id = ?", [orderId], async function (delErr) {
          if (delErr) {
            callback({ code: grpc.status.INTERNAL, details: 'Internal Server Error' });
          } else {
            const event = {
              type: 'ORDER_CANCELLED',
              orderId: orderId,
              productId: row.product_id,
              userId: row.user_id,
              quantity: row.quantity
            };
            try {
              await producer.send({
                topic: 'order-events',
                messages: [{ value: JSON.stringify(event) }],
              });
              console.log(`Event ORDER_CANCELLED sent for order ${orderId}`);
            } catch (kafkaErr) {
              console.error('Failed to send Kafka event:', kafkaErr);
            }
            callback(null, { success: true });
          }
        });
      }
    });
  }
});

const PORT = '50053';
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error(err);
    return;
  }
  server.start();
  console.log(`Orders Service running at http://0.0.0.0:${port}`);
  initKafka();
});
