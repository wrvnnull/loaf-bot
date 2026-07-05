# Loaf Markets Auto Trading Bot (GitHub Actions) — All Live Properties

Bot market-making untuk [Loaf Markets](https://api.loafmarkets.com), mengikuti
alur di dokumentasi resmi: `nonce -> place -> cancel`, plus strategy sketch
(quote di dalam best bid/ask, reconcile, requote, stop saat risk event) —
dijalankan **untuk SEMUA property yang statusnya `LIVE`**, ditemukan real-time
tiap siklus lewat `GET /api/trade` (endpoint publik, tidak butuh API key).

Confirmed dari respons `GET /api/trade` di production (`api.loafmarkets.com`):

| tokenName | asset | propertyId |
| --- | --- | --- |
| `opera` | Sydney Opera House | 1 |
| `eiffel` | Eiffel Tower | 2 |
| `liberty` | Statue of Liberty | 3 |
| `rainier` | Data Centre (AWS) | 5 |
| `monaco` | Circuit de Monaco | 6 |
| `marina` | Marina Bay Sands | 7 |
| `goldengate` | Golden Gate Bridge | 8 |
| `deepwaterbay` | 79 Deep Water Bay Rd | 9 |
| `metlife` | MetLife Stadium | 10 |

Bot **tidak hardcode daftar ini** — setiap siklus, bot fetch ulang
`GET /api/trade` dan trading semua property berstatus `LIVE` saat itu. Kalau
ada listing baru atau delisting, bot otomatis ikut menyesuaikan.

## ⚠️ Baca dulu sebelum jalan

- Default `LOAF_API_BASE` sekarang **`https://api.loafmarkets.com`** — ini
  host production sesuai reference docs. Cek dulu status trading akun kamu
  di web app sebelum jalanin dengan size besar.
- Akun kamu harus sudah **trading enabled** (referral code / kompetisi), kalau
  belum, API order akan balas `403`.
- **Order book sekarang diambil lewat WebSocket, bukan REST.** Ditemukan lewat
  testing: `GET /api/trade/:tokenName` ternyata **bukan** order book snapshot
  (asumsi awal dari tutorial salah) — endpoint itu cuma balikin daftar
  metadata property (`propertyList`), sama sekali tanpa `bids`/`asks`. Book
  data yang benar-benar ada cuma didokumentasikan lewat WebSocket
  (`orderbook:{propertyId}` channel). **Format frame untuk *subscribe* ke
  channel itu sendiri tidak didokumentasikan secara eksplisit** (cuma bentuk
  pesan `orderbook_update` yang *masuk* yang didokumentasikan) — `bot.py`
  mengirim frame tebakan `{"type": "subscribe", "channel": "orderbook:<id>"}`.
  **Ini best-effort, belum terverifikasi.** Kalau di log Action kamu lihat
  `"No orderbook_update received via WebSocket for propertyIds: [...]"`
  terus-menerus, cari baris `[WS DEBUG]` di log yang sama (isinya pesan
  mentah dari server atau error koneksi) dan kirim ke saya — dari situ saya
  bisa perbaiki format subscribe-nya.
- Trading SEMUA property live berarti jumlah API call per siklus jadi
  `~2 x jumlah_property` (fetch book + reconcile/requote). Kalau propertinya
  bertambah banyak, naikkan `TICK_SLEEP_SECONDS` atau batasi lewat
  `PROPERTY_ALLOWLIST` supaya tidak kena rate limit.
- **GitHub Actions tidak bisa loop selamanya.** Job dan cron minimal tiap 5
  menit. Bot loop internal ~4 menit per eksekusi (`RUN_DURATION_SECONDS`),
  lalu keluar bersih; workflow di-trigger ulang otomatis tiap 5 menit oleh
  GitHub. Efeknya "hampir terus-menerus", bukan satu proses yang hidup abadi.
- Ini kode contoh/referensi, bukan nasihat keuangan. Trading multi-aset
  otomatis ada risiko rugi yang bisa terjadi di banyak posisi sekaligus. Mulai
  dengan `PROPERTY_ALLOWLIST` berisi 1-2 token dan size kecil dulu.

## Struktur

```
bot.py                              # logic bot (discover properties, fetch book, quote, requote, risk checks)
requirements.txt
.github/workflows/trading-bot.yml   # jadwal otomatis tiap 5 menit + trigger manual
```

## Setup

1. Buat repo GitHub baru, push semua file ini ke sana.
2. Buat API key di Loaf web app (bagian API settings). Simpan secret-nya
   (hanya muncul sekali).
3. Di repo GitHub: **Settings → Secrets and variables → Actions**.

   **Secrets** (data sensitif):
   | Name | Contoh nilai |
   | --- | --- |
   | `LOAF_API_KEY` | (secret key dari step 2) |
   | `LOAF_API_BASE` | `https://api.loafmarkets.com` |

   **Variables** (opsional, boleh dikosongkan untuk pakai default di `bot.py`):
   | Name | Default | Arti |
   | --- | --- | --- |
   | `PROPERTY_ALLOWLIST` | *(kosong = semua LIVE)* | batasi ke token tertentu, contoh: `opera,eiffel` |
   | `TICK_SIZE_PCT` | `0.001` (0.1%) | jarak quote dari best bid/ask, sebagai % dari mid price |
   | `ORDER_SIZE` | `1` | ukuran tiap order, per property |
   | `MAX_INVENTORY` | `10` | batas inventory bersih (long/short), per property |
   | `MAX_SPREAD_PCT` | `0.05` | stop quoting kalau spread > 5% dari mid |
   | `REQUOTE_TOLERANCE_PCT` | `0.002` (0.2%) | requote kalau harga book bergeser lebih dari ini |
   | `TICK_SLEEP_SECONDS` | `10` | jeda antar siklus di dalam satu run |
   | `RUN_DURATION_SECONDS` | `240` | lama loop internal per eksekusi (< 5 menit) |

4. Aktifkan Actions di tab **Actions** repo kamu (kalau belum otomatis aktif).
5. **Disarankan:** set `PROPERTY_ALLOWLIST` ke 1 token dulu (misal `opera`)
   dan `RUN_DURATION_SECONDS` kecil (misal `30`), lalu trigger manual di tab
   **Actions → Loaf Trading Bot → Run workflow** untuk cek log-nya dulu
   sebelum melepas ke semua property / jadwal otomatis.

## Cara kerja bot (ringkas)

Setiap pass (`run_pass` di `bot.py`):
1. `GET /api/trade` → daftar semua property, filter yang `status == "LIVE"`
   (dan filter `PROPERTY_ALLOWLIST` kalau diisi).
2. `GET /api/history/orders/active` sekali (bukan per-property, untuk hemat
   API call), lalu difilter per `propertyId` secara lokal.
3. Sekali per pass: koneksi WebSocket (`wss://.../ws`), subscribe channel
   `orderbook:{propertyId}` untuk semua property, kumpulkan snapshot book
   pertama yang masuk untuk tiap property (timeout `WS_SUBSCRIBE_TIMEOUT`
   detik, default 8), lalu putus koneksi.
4. Untuk tiap property:
   - Kalau salah satu sisi book kosong atau spread terlalu lebar → skip
     property ini untuk siklus ini (risk event); property lain tetap jalan.
   - Hitung quote: bid = best_bid + tick, ask = best_ask - tick (tick = %
     dari mid, supaya masuk akal baik untuk token ~$97 maupun ~$1189).
   - Kalau quote lama sudah melenceng dari target → cancel, lalu pasang
     order baru (nonce baru tiap order, sesuai wajib di dokumentasi).
   - Cap inventory per property: tidak menambah bid/ask kalau sudah kena
     `MAX_INVENTORY` untuk property itu.
4. Sleep `TICK_SLEEP_SECONDS`, ulangi dari langkah 1 sampai
   `RUN_DURATION_SECONDS` habis, lalu keluar — dilanjutkan lagi oleh trigger
   cron berikutnya.

## Mematikan bot

- Cara cepat: disable workflow di tab **Actions** (klik "..." pada workflow →
  **Disable workflow**).
- Untuk membersihkan semua order yang masih terbuka di semua property,
  panggil manual `POST /api/orders/cancel-all` (lihat dokumentasi Orders
  API) — endpoint ini membatalkan semua order terbuka milik akun, tidak
  perlu per-property.

## Rekomendasi sebelum pakai dana sungguhan / semua property

- Uji dulu dengan `PROPERTY_ALLOWLIST` berisi 1 token, size kecil, beberapa
  hari, sebelum melepas ke semua property.
- Verifikasi shape respons `GET /api/trade/:tokenName` cocok dengan yang
  diasumsikan `best_bid_ask()` — cek log Action untuk pesan
  "missing book side" yang berulang.
- Tambahkan alerting (mis. notifikasi kalau satu property gagal berkali-kali).
- Simpan API key hanya sebagai GitHub Secret, jangan pernah commit ke kode.
- Cek ulang batas KYC, position limit, dan aturan yurisdiksi sebelum
  menjalankan dengan capital nyata di banyak aset sekaligus.
