const { createRxDatabase, addRxPlugin } = require('rxdb');
const { getRxStorageMemory } = require('rxdb/plugins/storage-memory');

const productSchema = {
  version: 0,
  primaryKey: 'id',
  type: 'object',
  properties: {
    id: { type: 'string', maxLength: 100 },
    name: { type: 'string' },
    description: { type: 'string' },
    price: { type: 'number' },
    stock: { type: 'number' } // For tracking inventory
  },
  required: ['id', 'name', 'price']
};

let dbPromise = null;

const getDatabase = async () => {
  if (!dbPromise) {
    dbPromise = createRxDatabase({
      name: 'productsdb',
      storage: getRxStorageMemory()
    }).then(async (db) => {
      await db.addCollections({
        products: {
          schema: productSchema
        }
      });
      return db;
    });
  }
  return dbPromise;
};

module.exports = { getDatabase };
