const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { getDatabase } = require('./database');
const { Kafka } = require('kafkajs');
const path = require('path');

const PROTO_PATH = path.resolve(__dirname, '../proto/products.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const productsProto = grpc.loadPackageDefinition(packageDefinition).products;

// Kafka Consumer Setup
const kafka = new Kafka({ clientId: 'products-service', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'products-group' });

const initKafka = async (db) => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'order-events', fromBeginning: true });
    console.log('Products Service connected to Kafka');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const event = JSON.parse(message.value.toString());
        if (event.type === 'ORDER_CREATED') {
          console.log(`Received ORDER_CREATED for product ${event.productId}`);
          const product = await db.products.findOne({ selector: { id: event.productId } }).exec();
          if (product) {
             const newStock = (product.stock || 0) - event.quantity;
             await product.incrementalPatch({ stock: newStock });
             console.log(`Stock for product ${event.productId} updated to ${newStock}`);
          }
        } else if (event.type === 'ORDER_CANCELLED') {
          console.log(`Received ORDER_CANCELLED for product ${event.productId}`);
          const product = await db.products.findOne({ selector: { id: event.productId } }).exec();
          if (product) {
             const newStock = (product.stock || 0) + event.quantity;
             await product.incrementalPatch({ stock: newStock });
             console.log(`Stock for product ${event.productId} restored to ${newStock}`);
          }
        }
      },
    });
  } catch (error) {
    console.error('Error connecting to Kafka:', error);
  }
};

const server = new grpc.Server();

const startServer = async () => {
  const db = await getDatabase();

  server.addService(productsProto.ProductService.service, {
    getProduct: async (call, callback) => {
      const product = await db.products.findOne({ selector: { id: call.request.id } }).exec();
      if (product) {
        callback(null, product.toJSON());
      } else {
        callback({ code: grpc.status.NOT_FOUND, details: 'Product not found' });
      }
    },
    searchProducts: async (call, callback) => {
      // Simple search (returns all for demo if query is empty)
      const products = await db.products.find().exec();
      callback(null, { products: products.map(p => p.toJSON()) });
    },
    createProduct: async (call, callback) => {
      const p = call.request;
      const id = Math.random().toString(36).substring(7);
      const newProduct = { id, name: p.name, description: p.description, price: p.price, stock: p.stock || 0 };
      await db.products.insert(newProduct);
      callback(null, newProduct);
    },
    updateProduct: async (call, callback) => {
      const p = call.request;
      const product = await db.products.findOne({ selector: { id: p.id } }).exec();
      if (product) {
        await product.incrementalPatch({
          name: p.name,
          description: p.description,
          price: p.price,
          stock: p.stock
        });
        callback(null, product.toJSON());
      } else {
        callback({ code: grpc.status.NOT_FOUND, details: 'Product not found' });
      }
    },
    deleteProduct: async (call, callback) => {
      const productId = call.request.id;
      const product = await db.products.findOne({ selector: { id: productId } }).exec();
      if (product) {
        await product.remove();
        callback(null, { success: true });
      } else {
        callback(null, { success: false });
      }
    },
  });

  const PORT = '50052';
  server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error(err);
      return;
    }
    server.start();
    console.log(`Products Service running at http://0.0.0.0:${port}`);
    initKafka(db); // Start kafka consumer
  });
};

startServer();
