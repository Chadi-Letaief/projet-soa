const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@as-integrations/express5');
const cors = require('cors');
const bodyParser = require('body-parser');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const app = express();
app.use(cors());
app.use(bodyParser.json());

// Load Protos
const userProtoPath = path.resolve(__dirname, '../proto/users.proto');
const productProtoPath = path.resolve(__dirname, '../proto/products.proto');
const orderProtoPath = path.resolve(__dirname, '../proto/orders.proto');

const loadProto = (protoPath) => protoLoader.loadSync(protoPath, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });

const userProto = grpc.loadPackageDefinition(loadProto(userProtoPath)).users;
const productProto = grpc.loadPackageDefinition(loadProto(productProtoPath)).products;
const orderProto = grpc.loadPackageDefinition(loadProto(orderProtoPath)).orders;

// Initialize gRPC Clients
const userClient = new userProto.UserService('localhost:50051', grpc.credentials.createInsecure());
const productClient = new productProto.ProductService('localhost:50052', grpc.credentials.createInsecure());
const orderClient = new orderProto.OrderService('localhost:50053', grpc.credentials.createInsecure());

// Promisify gRPC calls for easier use in REST and GraphQL
const grpcCall = (client, method, request) => {
  return new Promise((resolve, reject) => {
    client[method](request, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
};

// ======================= REST API =======================

// Users
app.get('/users/:id', async (req, res) => {
  try {
    const user = await grpcCall(userClient, 'getUser', { id: req.params.id });
    res.json(user);
  } catch (err) {
    res.status(500).json({ error: err.details });
  }
});
app.post('/users', async (req, res) => {
  try {
    const user = await grpcCall(userClient, 'createUser', req.body);
    res.json(user);
  } catch (err) {
    res.status(500).json({ error: err.details });
  }
});

// Products
app.get('/products/:id', async (req, res) => {
  try {
    const product = await grpcCall(productClient, 'getProduct', { id: req.params.id });
    res.json(product);
  } catch (err) {
    res.status(500).json({ error: err.details });
  }
});
app.get('/products', async (req, res) => {
  try {
    const products = await grpcCall(productClient, 'searchProducts', { query: req.query.q || '' });
    res.json(products);
  } catch (err) {
    res.status(500).json({ error: err.details });
  }
});
app.post('/products', async (req, res) => {
  try {
    const product = await grpcCall(productClient, 'createProduct', req.body);
    res.json(product);
  } catch (err) {
    res.status(500).json({ error: err.details });
  }
});

// Orders
app.get('/orders/:id', async (req, res) => {
  try {
    const order = await grpcCall(orderClient, 'getOrder', { id: req.params.id });
    res.json(order);
  } catch (err) {
    res.status(500).json({ error: err.details });
  }
});
app.post('/orders', async (req, res) => {
  try {
    const order = await grpcCall(orderClient, 'createOrder', req.body);
    res.json(order);
  } catch (err) {
    res.status(500).json({ error: err.details });
  }
});

// ===================== GraphQL API =====================
const typeDefs = `#graphql
  type User { id: String!, name: String!, email: String! }
  type Product { id: String!, name: String!, description: String!, price: Float!, stock: Int! }
  type Order { id: String!, user_id: String!, product_id: String!, quantity: Int!, status: String! }

  type Query {
    getUser(id: String!): User
    getProduct(id: String!): Product
    getProducts: [Product]
    getOrder(id: String!): Order
  }

  type Mutation {
    createUser(name: String!, email: String!): User
    createProduct(name: String!, description: String!, price: Float!, stock: Int!): Product
    createOrder(user_id: String!, product_id: String!, quantity: Int!): Order
  }
`;

const resolvers = {
  Query: {
    getUser: (_, { id }) => grpcCall(userClient, 'getUser', { id }),
    getProduct: (_, { id }) => grpcCall(productClient, 'getProduct', { id }),
    getProducts: async () => {
        const res = await grpcCall(productClient, 'searchProducts', { query: '' });
        return res.products;
    },
    getOrder: (_, { id }) => grpcCall(orderClient, 'getOrder', { id }),
  },
  Mutation: {
    createUser: (_, args) => grpcCall(userClient, 'createUser', args),
    createProduct: (_, args) => grpcCall(productClient, 'createProduct', args),
    createOrder: (_, args) => grpcCall(orderClient, 'createOrder', args),
  }
};

const startApolloServer = async () => {
  const server = new ApolloServer({ typeDefs, resolvers });
  await server.start();
  app.use('/graphql', expressMiddleware(server));

  const PORT = 3000;
  app.listen(PORT, () => {
    console.log(`API Gateway REST running at http://localhost:${PORT}`);
    console.log(`API Gateway GraphQL running at http://localhost:${PORT}/graphql`);
  });
};

startApolloServer();
