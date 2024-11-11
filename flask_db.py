import csv
import os
from collections import deque
from threading import Lock
from typing import Optional, List, Dict, Tuple
from flask import Flask, jsonify, request, render_template_string
from math import ceil
import random

class Tabela:
    def __init__(self, nome, campos, indice_campo=None, cache_limit=100):
        self.nome = nome
        self.campos = campos
        self.indice_campo = indice_campo
        self.lock = Lock()
        self.cache_limit = cache_limit
        self.cache = {}  # Cache para páginas de resultados {page_number: [data]}
        self.cache_access = deque(maxlen=self.cache_limit)  # Controla páginas no cache com limite de memória
        self.arquivo_csv = f"{nome}.csv"
        
        if not os.path.exists(self.arquivo_csv):
            with open(self.arquivo_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.campos)
                writer.writeheader()

    def _salvar_dados(self, linha):
        with open(self.arquivo_csv, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.campos)
            writer.writerow(linha)

    def _buscar_no_csv_paginado(self, page: int, per_page: int) -> Tuple[List[Dict[str, str]], int]:
        """Busca diretamente no CSV se a página solicitada não estiver no cache."""
        with open(self.arquivo_csv, 'r') as f:
            reader = list(csv.DictReader(f))
            total_items = len(reader)
            start = (page - 1) * per_page
            end = start + per_page
            page_data = reader[start:end]
            return page_data, total_items

    def _limpar_cache(self):
        """Limpa o cache de páginas."""
        self.cache.clear()
        self.cache_access.clear()

    def inserir(self, dados: Dict[str, str]):
        with self.lock:
            if not all(campo in dados for campo in self.campos):
                raise ValueError("Dados inválidos. Faltam campos.")
            self._salvar_dados(dados)
            self._limpar_cache()  # Limpar cache ao inserir

    def buscar_paginado(self, page: int, per_page: int) -> Tuple[List[Dict[str, str]], int, int]:
        """Retorna uma página de dados, primeiro tentando o cache e, se necessário, buscando no CSV."""
        cache_key = (page, per_page)
        
        # Tentar encontrar no cache
        if cache_key in self.cache:
            print(f"Cache hit for page {page}")
            return self.cache[cache_key], page, len(self.cache)

        # Buscar no CSV se não estiver no cache
        page_data, total_items = self._buscar_no_csv_paginado(page, per_page)
        
        # Armazenar a página no cache
        self.cache[cache_key] = page_data
        self.cache_access.append(cache_key)  # Manter controle de limite no cache

        # Limitar o tamanho do cache
        if len(self.cache_access) > self.cache_limit:
            oldest_key = self.cache_access.popleft()
            del self.cache[oldest_key]

        return page_data, page, ceil(total_items / per_page)
    
    def buscar_por_nome_paginado(self, nome: str, page: int, per_page: int) -> Tuple[List[Dict[str, str]], int, int]:
        """Busca usuários por nome com paginação."""
        cache_key = (nome, page, per_page)
        
        # Tentar encontrar no cache
        if cache_key in self.cache:
            print(f"Cache hit for search by name: {nome}, page {page}")
            return self.cache[cache_key], page, len(self.cache)

        # Buscar no CSV
        with open(self.arquivo_csv, 'r') as f:
            reader = list(csv.DictReader(f))
            filtro_nome = [row for row in reader if nome.lower() in row['nome'].lower()]
            total_items = len(filtro_nome)
            start = (page - 1) * per_page
            end = start + per_page
            page_data = filtro_nome[start:end]
            
        # Armazenar a página no cache
        self.cache[cache_key] = page_data
        self.cache_access.append(cache_key)

        # Limitar o tamanho do cache
        if len(self.cache_access) > self.cache_limit:
            oldest_key = self.cache_access.popleft()
            del self.cache[oldest_key]

        return page_data, page, ceil(total_items / per_page)


    def atualizar(self, campo_busca: str, valor_busca: str, novos_dados: Dict[str, str]):
        with self.lock:
            linhas_atualizadas = 0
            novas_linhas = []
            with open(self.arquivo_csv, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get(campo_busca) == valor_busca:
                        for k, v in novos_dados.items():
                            row[k] = v
                        linhas_atualizadas += 1
                    novas_linhas.append(row)
            with open(self.arquivo_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.campos)
                writer.writeheader()
                writer.writerows(novas_linhas)
            self._limpar_cache()  # Limpar cache ao atualizar
            return linhas_atualizadas

    def deletar(self, campo: str, valor: str):
        with self.lock:
            novas_linhas = []
            linhas_deletadas = 0
            with open(self.arquivo_csv, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get(campo) == valor:
                        linhas_deletadas += 1
                        continue
                    novas_linhas.append(row)
            with open(self.arquivo_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.campos)
                writer.writeheader()
                writer.writerows(novas_linhas)
            self._limpar_cache()  # Limpar cache ao deletar
            return linhas_deletadas

app = Flask(__name__)
usuarios = Tabela("usuarios", ["id", "nome", "idade"], indice_campo="id", cache_limit=10)

# Inserir 1000 usuários de teste
for i in range(1, 1001):
    usuarios.inserir({
        "id": str(i),
        "nome": f"User {i}",
        "idade": str(random.randint(18, 60))
    })


html_template = """
<!doctype html>
<html lang="pt-br">
<head>
    <meta charset="utf-8">
    <title>Gerenciamento de Usuários</title>
    <script>
        let currentPage = 1;
        const perPage = 10;
        let currentSearchPage = 1;

        async function buscarUsuarios(page = 1) {
            let response = await fetch(`/usuarios?page=${page}&per_page=${perPage}`);
            let data = await response.json();
            document.getElementById('usuarios').innerText = JSON.stringify(data.data, null, 2);
            document.getElementById('page-info').innerText = `Página ${data.page} de ${data.total_pages}`;
            currentPage = data.page;
        }

        async function buscarUsuariosPorNome(page = 1) {
            const nome = document.getElementById('buscar_nome').value;
            let response = await fetch(`/usuarios/buscar?nome=${nome}&page=${page}&per_page=${perPage}`);
            let data = await response.json();
            document.getElementById('usuarios').innerText = JSON.stringify(data.data, null, 2);
            document.getElementById('page-info').innerText = `Página ${data.page} de ${data.total_pages}`;
            currentSearchPage = data.page;
        }

        async function inserirUsuario() {
            const nome = document.getElementById('nome').value;
            const idade = document.getElementById('idade').value;
            await fetch('/usuarios', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({id: Date.now().toString(), nome, idade})
            });
            buscarUsuarios(currentPage);
        }

        async function atualizarUsuario() {
            const id = document.getElementById('update_id').value;
            const nome = document.getElementById('update_nome').value;
            const idade = document.getElementById('update_idade').value;
            await fetch(`/usuarios/${id}`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({nome, idade})
            });
            buscarUsuarios(currentPage);
        }

        async function deletarUsuario() {
            const id = document.getElementById('delete_id').value;
            await fetch(`/usuarios/${id}`, {method: 'DELETE'});
            buscarUsuarios(currentPage);
        }

        function proximaPagina() {
            if (currentSearchPage) {
                buscarUsuariosPorNome(currentSearchPage + 1);
            } else {
                buscarUsuarios(currentPage + 1);
            }
        }

        function paginaAnterior() {
            if (currentSearchPage && currentSearchPage > 1) {
                buscarUsuariosPorNome(currentSearchPage - 1);
            } else if (currentPage > 1) {
                buscarUsuarios(currentPage - 1);
            }
        }

        document.addEventListener("DOMContentLoaded", function() {
            buscarUsuarios();
        });
    </script>
</head>
<body>
    <h1>Gerenciamento de Usuários</h1>

    <h2>Inserir Usuário</h2>
    Nome: <input type="text" id="nome"><br>
    Idade: <input type="text" id="idade"><br>
    <button onclick="inserirUsuario()">Inserir</button>

    <h2>Atualizar Usuário</h2>
    ID: <input type="text" id="update_id"><br>
    Nome: <input type="text" id="update_nome"><br>
    Idade: <input type="text" id="update_idade"><br>
    <button onclick="atualizarUsuario()">Atualizar</button>

    <h2>Deletar Usuário</h2>
    ID: <input type="text" id="delete_id"><br>
    <button onclick="deletarUsuario()">Deletar</button>

    <h2>Buscar Usuário</h2>
    Nome: <input type="text" id="buscar_nome"><br>
    <button onclick="buscarUsuariosPorNome()">Buscar</button>

    <h2>Lista de Usuários</h2>
    <pre id="usuarios"></pre>
    <div>
        <button onclick="paginaAnterior()">Página Anterior</button>
        <button onclick="proximaPagina()">Próxima Página</button>
        <p id="page-info"></p>
    </div>
</body>
</html>
"""


@app.route('/')
def index():
    return render_template_string(html_template)

@app.route('/usuarios', methods=['POST'])
def inserir_usuario():
    dados = request.json
    try:
        usuarios.inserir(dados)
        return jsonify({"message": "Usuário inserido com sucesso."}), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

@app.route('/usuarios', methods=['GET'])
def buscar_usuarios():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    dados, page, total_pages = usuarios.buscar_paginado(page, per_page)
    return jsonify({
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "data": dados
    })

@app.route('/usuarios/buscar', methods=['GET'])
def buscar_usuarios_por_nome():
    nome = request.args.get('nome', '')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    dados, page, total_pages = usuarios.buscar_por_nome_paginado(nome, page, per_page)
    
    return jsonify({
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "data": dados
    })

@app.route('/usuarios/<id>', methods=['PUT'])
def atualizar_usuario(id):
    novos_dados = request.json
    atualizado = usuarios.atualizar("id", id, novos_dados)
    if atualizado:
        return jsonify({"message": f"{atualizado} usuário(s) atualizado(s)."})
    else:
        return jsonify({"error": "Usuário não encontrado para atualizar."}), 404

@app.route('/usuarios/<id>', methods=['DELETE'])
def deletar_usuario(id):
    deletado = usuarios.deletar("id", id)
    if deletado:
        return jsonify({"message": f"{deletado} usuário(s) deletado(s)."})
    else:
        return jsonify({"error": "Usuário não encontrado para deletar."}), 404

if __name__ == '__main__':
    app.run(debug=True)
