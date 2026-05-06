# 全国猛暑日マップ 2016–2025 — GitHub Pages

気象庁公開データを使い、2016〜2025年の猛暑日（最高気温35°C以上）を全国の観測地点について集計・可視化したサイトです。

## ファイル構成

```
pages/              ← このフォルダをそのままリポジトリのルートに置く
├── index.html          # ランディングページ（統計・グラフ・ランキング）
├── hot_days_map.html   # Folium インタラクティブ地図（自己完結 HTML）
├── figures/
│   ├── national_trend.png      # 全国年別猛暑日数推移
│   ├── top_stations.png        # 猛暑日数ランキング上位20地点
│   └── prefecture_summary.png  # 都道府県別サマリー
└── README.md           # このファイル
```

## GitHub Pages として公開する手順

1. **新リポジトリを作成**（例：`hot-days-map`）

2. **このフォルダの中身をリポジトリのルートにコピー**して push

   ```bash
   cp -r pages/* /path/to/hot-days-map/
   cd /path/to/hot-days-map
   git init
   git add .
   git commit -m "feat: initial publish of hot-days map"
   git remote add origin https://github.com/<username>/hot-days-map.git
   git push -u origin main
   ```

3. **GitHub Pages を有効化**

   - リポジトリの Settings → Pages
   - Source: `Deploy from a branch`
   - Branch: `main` / `/ (root)`
   - Save

4. しばらくすると `https://<username>.github.io/hot-days-map/` で公開される

## データについて

- **データソース**：[気象庁 過去の気象データ](https://www.data.jma.go.jp/obd/stats/etrn/)
- **対象期間**：2016〜2025年
- **猛暑日の定義**：日最高気温 35°C以上（気象庁公式）
- **解析環境**：Python 3.13 / httpx / BeautifulSoup4 / pandas / Folium / matplotlib
