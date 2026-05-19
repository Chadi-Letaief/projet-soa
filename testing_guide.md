# Guide de Test : Marketplace E-commerce (Projet SOA)

Ce guide vous explique étape par étape comment tester tous les aspects de votre architecture orientée services (REST, GraphQL, gRPC, bases de données et communication asynchrone via Kafka).

---

## 🏗️ Rappel de l'Architecture
*   **API Gateway (Port 3000)** : Point d'accès unique. Traduit le REST / GraphQL en appels gRPC.
*   **Users Service (Port 50051)** : Gère les utilisateurs avec **SQLite3**.
*   **Products Service (Port 50052)** : Gère le catalogue et les stocks avec **RxDB (NoSQL)**. Consomme les événements Kafka.
*   **Orders Service (Port 50053)** : Gère les commandes avec **SQLite3**. Produit des événements Kafka lors des ventes.

---

## 1. Tests de l'interface REST API (Postman ou cURL)

Vous pouvez tester l'ensemble du cycle d'achat classique (Création de compte -> Ajout de produit -> Commande -> Mise à jour du stock).

### Étape 1.1 : Créer un utilisateur
*   **Méthode :** `POST`
*   **URL :** `http://localhost:3000/users`
*   **En-têtes (Headers) :** `Content-Type: application/json`
*   **Corps (Body - JSON) :**
    ```json
    {
      "name": "Jane Doe",
      "email": "jane.doe@example.com"
    }
    ```
*   *Notez l'ID (`id`) généré dans la réponse.*

---

### Étape 1.2 : Créer un produit dans le catalogue
*   **Méthode :** `POST`
*   **URL :** `http://localhost:3000/products`
*   **En-têtes (Headers) :** `Content-Type: application/json`
*   **Corps (Body - JSON) :**
    ```json
    {
      "name": "Clavier Gamer RGB",
      "description": "Clavier mécanique rétroéclairé switchs rouges",
      "price": 89.99,
      "stock": 10
    }
    ```
*   *Notez l'ID du produit généré.*

---

### Étape 1.3 : Consulter le catalogue
*   **Méthode :** `GET`
*   **URL :** `http://localhost:3000/products`
*   *Vérifiez que votre clavier s'affiche bien dans la liste avec un stock de 10.*

---

### Étape 1.4 : Passer une commande (Déclenche le flux Kafka !)
*   **Méthode :** `POST`
*   **URL :** `http://localhost:3000/orders`
*   **En-têtes (Headers) :** `Content-Type: application/json`
*   **Corps (Body - JSON) :**
    ```json
    {
      "user_id": "METTRE_ICI_L_ID_UTILISATEUR",
      "product_id": "METTRE_ICI_L_ID_PRODUIT",
      "quantity": 2
    }
    ```

---

### Étape 1.5 : Vérifier la mise à jour asynchrone (Kafka 🚀)
Le service **Orders** a enregistré la commande et a immédiatement envoyé un événement Kafka.
Le service **Products** a reçu cet événement et a réduit le stock.

1.  **Regardez les logs dans vos terminaux :**
    *   Le terminal d'**Orders** affiche qu'il a publié l'événement.
    *   Le terminal de **Products** affiche qu'il a traité la commande et mis à jour le stock.
2.  **Faites un nouveau `GET http://localhost:3000/products` :**
    *   Le stock de votre clavier doit maintenant être passé de **10** à **8** !

---

## 2. Tests de l'interface GraphQL (Bac à sable Apollo)

Votre API Gateway intègre un serveur GraphQL complet pour vos requêtes complexes.

1.  Ouvrez votre navigateur sur : [http://localhost:3000/graphql](http://localhost:3000/graphql)
2.  Cliquez sur **"Query your server"** pour ouvrir le Sandbox Apollo.
3.  Vous pouvez exécuter les requêtes suivantes dans l'éditeur :

### Créer un utilisateur (Mutation)
```graphql
mutation CreateUser {
  createUser(name: "Alice Smith", email: "alice@smith.com") {
    id
    name
    email
  }
}
```

### Créer un produit (Mutation)
```graphql
mutation CreateProduct {
  createProduct(name: "Souris Sans Fil", description: "Souris optique ergonomique", price: 45.50, stock: 20) {
    id
    name
    price
    stock
  }
}
```

### Récupérer tous les produits (Query)
```graphql
query GetProducts {
  getProducts {
    id
    name
    description
    price
    stock
  }
}
```

### Passer une commande (Mutation)
```graphql
mutation CreateOrder {
  createOrder(
    user_id: "METTRE_L_ID_D_ALICE",
    product_id: "METTRE_L_ID_DE_LA_SOURIS",
    quantity: 1
  ) {
    id
    status
    quantity
  }
}
```

---

## 💡 Conseils pour votre Présentation Académique
Pour impressionner vos jurys/professeurs, montrez le découpage et le couplage faible :
1.  **Démontrez la tolérance aux pannes :** Éteignez le service **Products** (fermez sa fenêtre). Passez une commande. La commande réussira toujours (car le service Orders est en ligne). Rallumez ensuite le service **Products** : il consommera le message Kafka en attente et mettra le stock à jour rétroactivement ! C'est le principe même de la résilience asynchrone de la SOA.
2.  **Montrez la diversité des bases de données :** Expliquez que vous utilisez du **Relationnel (SQL)** pour les utilisateurs et commandes (cohérence forte) et du **NoSQL (RxDB)** pour les produits (flexibilité).
