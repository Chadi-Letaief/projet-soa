# Projet SOA - Marketplace E-commerce

Ce projet est une application basée sur une architecture microservices utilisant Node.js, répondant aux exigences du cours SOA. L'application simule une marketplace d'e-commerce simplifiée avec une gestion d'utilisateurs, de catalogue de produits, et de commandes.

## 1. Description du projet
Le système est composé de trois microservices indépendants, d'une API Gateway servant de point d'entrée unique, d'un broker Kafka pour la communication asynchrone, et de bases de données isolées pour chaque microservice (SQLite3 et RxDB).

## 2. Schéma d'Architecture
```mermaid
graph TD
    Client[Client Web/Mobile] -->|REST / GraphQL| API_Gateway[API Gateway]
    
    API_Gateway -->|gRPC| Users_Service[Users Service]
    API_Gateway -->|gRPC| Products_Service[Products Service]:32 PM
    
    API_Gateway -->|gRPC| Orders_Service[Orders Service]

    Users_Service --> Users_DB[(SQLite3: Users DB)]
    Products_Service --> Products_DB[(RxDB: Products DB)]
    Orders_Service --> Orders_DB[(SQLite3: Orders DB)]

    Orders_Service -->|Publie 'order-created'| Kafka[Broker Kafka]
    Kafka -->|Consomme 'order-created'| Products_Service
```

## 3. Microservices et Bases de données
- **Users Service** (`:50051`) : Gère les comptes utilisateurs. Base de données : **SQLite3**.
- **Products Service** (`:50052`) : Gère le catalogue et les stocks. Base de données : **RxDB** (NoSQL InMemory Adapter).
- **Orders Service** (`:50053`) : Gère les commandes. Base de données : **SQLite3**.

Toute la communication synchrone entre l'API Gateway et les microservices se fait via **gRPC**. Les contrats (`.proto`) sont définis dans le répertoire `/proto`.

## 4. Endpoints REST (API Gateway - Port 3000)
L'API Gateway expose des endpoints REST pour interagir avec le système :

- **Users**
  - `GET /users/:id` : Récupère un utilisateur.
  - `POST /users` : Crée un utilisateur. (Body: `{ "name": "...", "email": "..." }`)
- **Products**
  - `GET /products` : Liste tous les produits.
  - `GET /products/:id` : Récupère un produit.
  - `POST /products` : Crée un produit. (Body: `{ "name": "...", "description": "...", "price": 10.5 }`)
- **Orders**
  - `GET /orders/:id` : Récupère une commande.
  - `POST /orders` : Crée une commande. (Body: `{ "user_id": "...", "product_id": "...", "quantity": 1 }`)

## 5. Schéma GraphQL (API Gateway - `/graphql`)
L'API Gateway expose également une interface GraphQL offrant plus de flexibilité :

**Queries :**
- `getUser(id: String!): User`
- `getProduct(id: String!): Product`
- `getProducts: [Product]`
- `getOrder(id: String!): Order`

**Mutations :**
- `createUser(name: String!, email: String!): User`
- `createProduct(name: String!, description: String!, price: Float!): Product`
- `createOrder(user_id: String!, product_id: String!, quantity: Int!): Order`

## 6. Kafka et Événements métier
- **Broker** : Kafka & Zookeeper (via Docker Compose).
- **Topic** : `order-events`.
- **Producteur** : `orders-service`. Lorsqu'une commande est créée (via REST ou GraphQL), le service enregistre la commande dans SQLite3 et publie un événement `ORDER_CREATED` contenant les informations de la commande (`orderId`, `productId`, `userId`, `quantity`).
- **Consommateur** : `products-service`. Il écoute le topic `order-events`. Lorsqu'il reçoit un événement `ORDER_CREATED`, il met à jour sa base RxDB en décrémentant le stock du produit correspondant. Cela permet de découpler la gestion des stocks de la création de la commande.

## 7. Instructions d'installation et d'exécution

### Prérequis
- Node.js installé.
- Docker et Docker Compose installés (pour Kafka).

### Étape 1 : Démarrer Kafka
À la racine du projet (`projet-soa`), lancez Kafka :
```bash
docker-compose up -d
```

### Étape 2 : Installer les dépendances
```bash
cd api-gateway && npm install
cd ../users-service && npm install
cd ../products-service && npm install
cd ../orders-service && npm install
```

### Étape 3 : Démarrer les services
Ouvrez 4 terminaux séparés à la racine de `projet-soa` :

Terminal 1 (Users) :
```bash
cd users-service && node index.js
```
Terminal 2 (Products) :
```bash
cd products-service && node index.js
```
Terminal 3 (Orders) :
```bash
cd orders-service && node index.js
```
Terminal 4 (API Gateway) :
```bash
cd api-gateway && node index.js
```

L'application est maintenant accessible via REST sur `http://localhost:3000` et via GraphQL sur `http://localhost:3000/graphql`.
