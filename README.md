# db_python
banco de dados em python puro

```python
# exemplo
# Definir tabela "usuarios" com campos e índice no campo "id"
usuarios = Tabela("usuarios", ["id", "nome", "idade"], indice_campo="id", cache_limit=10)

# Inserir dados
usuarios.inserir({"id": "1", "nome": "Alice", "idade": "30"})
usuarios.inserir({"id": "2", "nome": "Bob", "idade": "25"})

# Buscar dados
resultado = usuarios.buscar("id", "1")
print("Busca por ID:", resultado)

# Atualizar dados
usuarios.atualizar("id", "1", {"nome": "Alice Smith"})
print("Após atualização:", usuarios.buscar("id", "1"))

# Deletar dados
usuarios.deletar("id", "2")
print("Após deleção:", usuarios.buscar("id", "2"))
```

obs: pode usar lista de dicionario nos valores dos campos
    
