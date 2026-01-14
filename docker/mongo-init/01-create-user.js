// Cria usuário para o banco cnpj
db = db.getSiblingDB('cnpj');

db.createUser({
  user: 'cnpj_user',
  pwd: 'cnpj123',
  roles: [
    { role: 'readWrite', db: 'cnpj' }
  ]
});

print('Usuário cnpj_user criado com sucesso!');
