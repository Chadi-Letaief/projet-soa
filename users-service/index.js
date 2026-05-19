const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const db = require('./database');
const path = require('path');
const { v4: uuidv4 } = require('uuid'); // Will need to install uuid if not already

const PROTO_PATH = path.resolve(__dirname, '../proto/users.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const usersProto = grpc.loadPackageDefinition(packageDefinition).users;

const server = new grpc.Server();

server.addService(usersProto.UserService.service, {
  getUser: (call, callback) => {
    const userId = call.request.id;
    db.get("SELECT * FROM users WHERE id = ?", [userId], (err, row) => {
      if (err) {
        callback({
          code: grpc.status.INTERNAL,
          details: 'Internal Server Error',
        });
      } else if (row) {
        callback(null, row);
      } else {
        callback({
          code: grpc.status.NOT_FOUND,
          details: 'User not found',
        });
      }
    });
  },
  createUser: (call, callback) => {
    const user = call.request;
    const userId = Math.random().toString(36).substring(7); // Simple UUID for demo
    db.run(
      "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
      [userId, user.name, user.email],
      function (err) {
        if (err) {
          callback({
            code: grpc.status.INTERNAL,
            details: 'Internal Server Error',
          });
        } else {
          callback(null, { id: userId, name: user.name, email: user.email });
        }
      }
    );
  },
});

const PORT = '50051';
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error(err);
    return;
  }
  server.start();
  console.log(`Users Service running at http://0.0.0.0:${port}`);
});
