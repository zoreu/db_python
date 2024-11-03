import csv
import os
from collections import deque
from threading import Lock
from typing import Optional, List, Dict

class Tabela:
    def __init__(self, nome, campos, indice_campo=None, cache_limit=100):
        self.nome = nome
        self.campos = campos
        self.indice_campo = indice_campo
        self.lock = Lock()
        self.cache_limit = cache_limit  # Limite de cache em memória
        self.cache = deque(maxlen=self.cache_limit)  # Cache para as linhas mais recentes
        self.arquivo_csv = f"{nome}.csv"
        
        # Carregar índice básico em disco se existir
        if not os.path.exists(self.arquivo_csv):
            with open(self.arquivo_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.campos)
                writer.writeheader()

    def _salvar_dados(self, linha):
        with open(self.arquivo_csv, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.campos)
            writer.writerow(linha)

    def _buscar_no_csv(self, campo, valor):
        """Busca uma linha diretamente no CSV usando uma busca linear, evita carregar tudo em memória."""
        with open(self.arquivo_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get(campo) == valor:
                    return row
        return None

    def inserir(self, dados: Dict[str, str]):
        """Insere dados no CSV e os adiciona ao cache, respeitando o limite de memória."""
        with self.lock:
            # Validar dados
            if not all(campo in dados for campo in self.campos):
                raise ValueError("Dados inválidos. Faltam campos.")

            # Inserir dados no arquivo CSV
            self._salvar_dados(dados)

            # Atualizar cache
            self.cache.append(dados)

    def buscar(self, campo: str, valor: str) -> Optional[Dict[str, str]]:
        """Busca um dado no cache e, se não encontrar, busca no CSV."""
        with self.lock:
            # Tentar encontrar no cache
            for linha in self.cache:
                if linha.get(campo) == valor:
                    return linha

            # Caso não esteja no cache, buscar no CSV
            resultado = self._buscar_no_csv(campo, valor)
            if resultado:
                # Adicionar ao cache (o mais recente fica no cache)
                self.cache.append(resultado)
            return resultado

    def atualizar(self, campo_busca: str, valor_busca: str, novos_dados: Dict[str, str]):
        """Atualiza uma linha no CSV e no cache."""
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
                        self.cache.append(row)  # Atualizar cache com a linha modificada
                    novas_linhas.append(row)

            # Sobrescrever o CSV com as linhas atualizadas
            with open(self.arquivo_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.campos)
                writer.writeheader()
                writer.writerows(novas_linhas)

            return linhas_atualizadas

    def deletar(self, campo: str, valor: str):
        """Remove uma linha do CSV e atualiza o cache."""
        with self.lock:
            novas_linhas = []
            linhas_deletadas = 0
            with open(self.arquivo_csv, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get(campo) == valor:
                        linhas_deletadas += 1
                        continue  # Pular linha a ser deletada
                    novas_linhas.append(row)

            # Sobrescrever o CSV com as linhas que restaram
            with open(self.arquivo_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.campos)
                writer.writeheader()
                writer.writerows(novas_linhas)

            # Atualizar o cache, removendo itens deletados
            self.cache = deque((linha for linha in self.cache if linha.get(campo) != valor), maxlen=self.cache_limit)
            return linhas_deletadas

# Exemplo de uso
if __name__ == "__main__":
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
