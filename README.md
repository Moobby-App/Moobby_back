# Mooby Backend — Development

Panduan menjalankan gateway Development dari repository ini, membuka akses sementara melalui ngrok, dan memindahkan setup ke Mac lain. Semua instruksi di halaman ini untuk Development; jangan gunakan bearer token bersama di aplikasi Release.

## Persiapan Mac baru

1. Clone repository dan checkout branch yang dipakai untuk Development.
2. Pasang Python 3.12 dan buat virtual environment:

   ```bash
   python3.12 -m venv .venv
   .venv/bin/python -m pip install --upgrade pip
   .venv/bin/python -m pip install -e .
   ```

3. Salin `.env.example` menjadi `.env`, lalu isi nilai Development yang sesuai. Jangan commit atau kirim `.env`; pindahkan rahasia melalui password manager atau secret manager. Gunakan login database `mooby_app_api_login_dev` khusus API, bukan login worker atau owner. `APP_API_BEARER_TOKEN` harus token acak Development minimal 32 karakter.
4. Pindahkan folder `evaluation/private/Scrape_Manual/baseline_artifacts/` melalui penyimpanan privat yang disetujui. Folder ini tidak ikut Git. Jangan kirim artefak model atau data privat lewat commit.

## Jalankan API lokal

Dari root repository, muat konfigurasi lokal tanpa menampilkan nilainya, lalu jalankan server:

```bash
set -a
source .env
set +a
.venv/bin/uvicorn mooby.api.app:app --host 127.0.0.1 --port 8000
```

Di terminal kedua, cek liveness:

```bash
curl -i http://127.0.0.1:8000/health/live
```

Liveness tidak memerlukan token. Readiness memerlukan bearer token yang sama dengan konfigurasi server:

```bash
curl -i \
  -H "Authorization: Bearer ${APP_API_BEARER_TOKEN}" \
  http://127.0.0.1:8000/health/ready
```

`200` dengan database dan projection `available` menandakan gateway siap. `503 source_unavailable` berarti ada konfigurasi atau dependensi yang belum siap; jangan menafsirkannya sebagai hasil pencarian kosong.

## Buka gateway Development melalui ngrok

Pasang CLI dan daftarkan authtoken pada Mac secara lokal. Authtoken ngrok disimpan oleh CLI di luar repository:

```bash
brew install ngrok
ngrok config add-authtoken "TOKEN_NGROK_PRIBADI"
```

Biarkan API tetap berjalan di terminal pertama. Di terminal kedua jalankan:

```bash
ngrok http 8000
```

Salin URL HTTPS yang ditampilkan ngrok, misalnya `https://contoh-acak.ngrok-free.app`. Gunakan URL tersebut sebagai base URL Development di Run Scheme Xcode pribadi:

```text
MOOBY_DEV_API_BASE_URL = https://contoh-acak.ngrok-free.app
MOOBY_DEV_API_TOKEN = nilai APP_API_BEARER_TOKEN dari .env backend
```

Nama variabel app adalah `MOOBY_DEV_API_TOKEN`; variabel server bernama `APP_API_BEARER_TOKEN`. Jangan menaruh token server di source, file proyek bersama, screenshot, atau Scheme yang dicentang `Shared`. Token embedded hanya untuk pengujian Debug pribadi. ngrok hanya menyediakan tunnel dan tidak menggantikan autentikasi gateway. Hentikan tunnel dengan `Ctrl-C` setelah tes.

URL ngrok gratis dapat berubah ketika tunnel dibuat ulang. Perbarui base URL lokal Xcode setiap kali URL berubah. Untuk URL tetap, atur domain Development di akun ngrok dan ikuti opsi `--url` pada dokumentasi resmi ngrok. Jangan menganggap URL tetap sebagai kontrol akses.

### Pindah laptop

Pada Mac baru, ulangi persiapan di atas, pasang authtoken ngrok secara lokal, isi `.env` dari sumber rahasia yang aman, dan pastikan artefak privat tersedia. Jalankan API, buka tunnel baru, lalu masukkan URL HTTPS yang baru ke Run Scheme Debug pribadi. Jangan menyalin direktori `.venv`; buat ulang dari `pyproject.toml`.

## Replay batch sintetis (simulasi lokal)

CSV `evaluation/private/synthetic_training_v1.csv` berisi teks tetapi tidak memiliki `published_at`. Skrip replay memakai waktu simulasi yang sama untuk semua baris agar tahapan intake dan NLP dapat dicoba. Timestamp dan provenance yang dibentuk skrip hanya ada di memori; keduanya bukan metadata sumber yang nyata.

Replay menjalankan pemrosesan backend atas kolom `raw_text`, mengabaikan label CSV, dan menulis ringkasan agregat tanpa teks. Skrip tidak membuka koneksi database, mengantrekan item, menulis event, atau membuat data terlihat di app. Feed normal `/api/v1/disruptions` tetap hanya berasal dari event yang memenuhi syarat `verified` dan `published`. Gunakan jalur ini untuk pemeriksaan batch classifier dan prefilter, bukan untuk mengisi kartu app.

```bash
cd /Users/naufal/Moobby_back
set -a
source .env
set +a
.venv/bin/python scripts/replay_synthetic_batch.py \
  --input /Users/naufal/Moobby_back/evaluation/private/synthetic_training_v1.csv \
  --confirm-simulation-only
```

Laporan hitungan disimpan di `evaluation/private/Scrape_Manual/synthetic_batch_replay_report.json` dan tetap berada di direktori yang diabaikan Git. Periksa hitungan dan reason code saja; jangan salin teks mentah ke log atau chat.

Untuk membuat contoh kartu UI, gunakan fixture/mock terpisah yang secara jelas diberi label **SIMULASI**. Jangan mengubah synthetic training text menjadi sumber collection atau mempublikasikannya sebagai laporan gangguan nyata.

### Mock test bersamaan dengan feed normal

API yang sama pada port 8000 menyediakan dua endpoint dengan bearer token yang sama:

- `/api/v1/disruptions`: selalu feed normal dari database.
- `/api/v1/mock/disruptions`: fixture sintetis di memori, hanya untuk Development.

Mock tidak mengganti feed normal, menjalankan worker, atau menulis data database.
Item mock berlabel **SIMULASI**. Keduanya menggunakan `station_ids`, pagination,
dan kontrak lokasi yang sama. App harus memilih path mock secara eksplisit untuk
uji visual, bukan mencampurkan itemnya dengan laporan nyata.

Untuk LaunchAgent yang sudah dikonfigurasi, tambahkan dua field berikut ke
`ops/launchd/development-api.json` lokal, sambil mempertahankan project ref, domain,
dan path ngrok yang sudah ada:

```json
"mock_enabled": true,
"mock_ttl_minutes": 60
```

Field tersebut bagian dari objek JSON yang sudah ada, bukan file JSON lengkap.
Default mock tetap nonaktif. TTL dibatasi 1–240 menit sejak proses API dimulai.
Restart hanya API saat siap menerapkan perubahan; worker dan ngrok tidak perlu
dihentikan. Untuk menonaktifkan mock, set `mock_enabled` menjadi `false` dan restart
API. Saat nonaktif/TTL habis, endpoint mock mengembalikan 404; feed normal tetap
membaca database.

Path uji melalui domain yang sudah dikonfigurasi:

```text
https://capsule-favored-eclair.ngrok-free.dev/api/v1/mock/disruptions?station_ids=open-krl:SRP&station_ids=open-krl:THB
```

Tetap sertakan bearer token melalui header, bukan query string. Lihat
[panduan API launchd](docs/development_api_launchd.md#mock-test-pada-port-dan-tunnel-yang-sama).
Untuk API manual, tiga flag `APP_API_ENVIRONMENT=development`,
`APP_API_SYNTHETIC_FIXTURE_MODE=development`, dan
`APP_API_SYNTHETIC_FIXTURE_TTL_MINUTES=60` kini hanya mengaktifkan endpoint mock.
Tidak ada lagi pengalihan otomatis endpoint feed normal ke simulasi.

## Kontrak dan batas saat ini

- App memanggil FastAPI melalui gateway; app tidak memakai koneksi PostgreSQL.
- API membaca `app_api.published_disruptions` menggunakan login read-only.
- Feed normal hanya memuat event `verified` + `published` yang masih berada dalam jendela 24 jam. Hasil processor baru dengan `result_status=succeeded` dan draft valid setelah embedding wajib berhasil disimpan atomik sebagai `verified`/`published`, tanpa review manusia, verifier terpisah, atau menunggu korelasi. `verified` berarti diterima mesin, bukan konfirmasi resmi. Status queue sukses saja tidak cukup; skip/error/non-disruption/tanpa draft valid tidak dipublikasikan. Draft lama tidak dipromosikan.
- Respons menyediakan `original_text` (teks sumber asli, nullable untuk event lama). Kutipan/offset bukti internal, metadata penulis terpisah, dan ID sumber privat tetap tidak dikirim.
- Detail endpoint dan status API ada di [`docs/app_api_development.md`](docs/app_api_development.md); aturan produk ada di [`Spec.md`](Spec.md).

Dokumentasi tunnel dan konfigurasi mengikuti [ngrok macOS setup](https://ngrok.com/download/mac-os?tab=download) dan [HTTP/S endpoint guide](https://ngrok.com/docs/gateway/endpoints/http).

### Publikasi NLP otomatis dengan embedding wajib

Worker membuat embedding hanya setelah hasil processor `succeeded` memiliki draft
valid. Setelah embedding berhasil, event, vector, provenance, evidence, run, dan
queue completion disimpan atomik sebagai `verified`/`published`. `verified` berarti
lolos pipeline mesin, bukan konfirmasi operator/resmi. Model tidak tersedia atau
vector tidak valid menghasilkan `transient_embedding_error`; event tidak ditulis.
Korelasi dan review manusia bukan syarat publikasi.

Migrasi 015 sudah tercatat di Git; status penerapannya tidak diketahui. Jangan
mengandalkan mode embedding opsionalnya. Pemilik perlu memeriksa Development
dan menerapkan forward migration 016 untuk mempertahankan NOT NULL serta
INSERT enam kolom yang diperlukan. Tidak ada migrasi diterapkan oleh perubahan ini.

Lihat [panduan embedding](docs/embedding_operations.md) untuk penyediaan model
lokal, batasan artefak, migrasi manual, dan query baca-saja.

Tes lokal tanpa database, environment rahasia, atau model privat:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

## Worker Development otomatis di Mac

CLI worker yang ada mendukung polling berkelanjutan. Setup LaunchAgent opt-in,
validasi project Development/login worker, lock satu instance, log aman, serta
langkah install/start/stop/status/uninstall ada di
[panduan launchd](docs/development_worker_launchd.md). Template belum dipasang atau
dijalankan; aktivasi akan memproses dan menyimpan hasil Development sungguhan.

## API dan tunnel Development otomatis di Mac

Template LaunchAgent terpisah untuk API localhost dan ngrok tersedia secara opt-in.
Lihat [panduan API/ngrok launchd](docs/development_api_launchd.md) untuk konfigurasi,
install/start/status/stop/uninstall, dan batas log. Domain ngrok stabil belum
ditemukan pada konfigurasi lokal saat persiapan; tunnel tetap diblokir sampai
pemilik mengonfirmasi domain yang sudah dikonfigurasi. Tidak ada service dipasang
atau dijalankan oleh perubahan ini.

### Field teks asli pada feed

Setiap item normal/mock sekarang memiliki `original_text` di samping `title` dan
`summary`. Untuk event normal baru, teks disalin apa adanya saat worker menyimpan
event; untuk mock, berasal dari input sintetis yang sama. Event lama bernilai
`null`; tidak ada backfill otomatis. Field ini tidak mengubah filter/lokasi/severity.

Migrasi `018_app_api_original_text.sql` harus diterapkan manual di Development
sebelum men-deploy ulang worker/API yang memakai kolom baru. Migrasi belum diterapkan
oleh perubahan kode ini; tidak ada koneksi atau perubahan data database. Lihat
[kontrak API](docs/app_api_development.md#original-source-text) untuk detail.
