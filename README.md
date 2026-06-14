# Market Terminal

あっぷるキャピタルグループ向けのマーケットモニターです。

このリポジトリは `G:\マイドライブ\22. AIプロジェクト\tools\market-terminal` を正とします。Cドライブ側の作業ディレクトリには依存しません。

## 構成

| Path | Role |
|---|---|
| `server.py` | Python標準ライブラリだけで動くAPI兼静的ファイルサーバー |
| `v3/` | マーケットモニター本体の静的フロントエンド |
| `v3/config.js` | GitHub Pagesなど別ドメイン配信時のAPIベースURL設定 |
| `instruments.json` | ウォッチリスト銘柄 |
| `news_sources.json` | ニュースRSSソース |
| `symbols.json` | 銘柄検索用ローカルマスター |
| `render.yaml` | Render Web Service向けデプロイ設定 |

## ローカル起動

```powershell
cd "G:\マイドライブ\22. AIプロジェクト\tools\market-terminal"
python server.py 8799
```

ブラウザで `http://127.0.0.1:8799/` を開きます。

## 公開デプロイ

### 推奨: RenderでフロントとAPIを同一URL公開

GitHubにこのリポジトリを作成してpushした後、RenderでBlueprintまたはWeb Serviceとして接続します。

`render.yaml` により以下で起動します。

```bash
python server.py
```

Renderは `PORT` 環境変数を自動で渡します。`server.py` はその値を使って起動します。

公開時はデフォルトで `MT_READONLY=1` としており、ウォッチリスト変更APIを無効化します。一般公開ではこの設定を推奨します。

### GitHub Pagesで静的フロントだけ公開する場合

GitHub Pagesは静的ホスティングなので、APIは別の公開サーバーが必要です。

`v3/config.js` を次のように変更します。

```js
window.MARKET_API_BASE = "https://your-market-terminal-api.onrender.com";
```

そのうえで `v3/` をGitHub Pagesの公開対象にします。

## 環境変数

| Name | Default | Description |
|---|---:|---|
| `PORT` | `8799` | 公開環境の待受ポート。Renderでは自動設定 |
| `MT_HOST` | `0.0.0.0` | 待受ホスト |
| `MT_READONLY` | off | `1`で銘柄追加・削除・並び替えAPIを無効化 |
| `MT_CORS_ORIGIN` | `*` | GitHub Pagesなど別オリジンからAPIを呼ぶ場合のCORS許可 |

## 確認コマンド

```powershell
python -m py_compile server.py
python server.py 8799
```

ヘルスチェック:

```text
GET /api/health
```
