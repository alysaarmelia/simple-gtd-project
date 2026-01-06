#!/bin/bash

# ==============================================================================
# GTD PROJECT - DISASTER RECOVERY MANAGER
# ==============================================================================
# Script ini menangani Backup & Restore untuk Database PostgreSQL dan MinIO Data
# Prasyarat: Harus dijalankan dari root folder project (tempat docker-compose.yaml berada)
# Usage: 
#   ./manage_disaster_recovery.sh backup
#   ./manage_disaster_recovery.sh restore <timestamp_folder>
# ==============================================================================

# --- KONFIGURASI ---
BACKUP_DIR="./backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
PG_USER="airflow"
PG_DB="airflow"
PG_SERVICE_NAME="postgres"  # Nama service di docker-compose.yaml
MINIO_DATA_DIR="./minio_data"

# Pastikan folder backup ada
mkdir -p "$BACKUP_DIR"

# Fungsi: Log dengan Warna
log_info() { echo -e "\e[34m[INFO]\e[0m $1"; }
log_success() { echo -e "\e[32m[SUCCESS]\e[0m $1"; }
log_error() { echo -e "\e[31m[ERROR]\e[0m $1"; }
log_warn() { echo -e "\e[33m[WARN]\e[0m $1"; }

# ==============================================================================
# 1. MODUL BACKUP
# ==============================================================================
run_backup() {
    log_info "Memulai proses Disaster Recovery Backup..."
    CURRENT_BACKUP_PATH="$BACKUP_DIR/$TIMESTAMP"
    mkdir -p "$CURRENT_BACKUP_PATH"

    # --- STEP A: BACKUP POSTGRESQL ---
    log_info "Backing up PostgreSQL Database..."
    
    # Cek apakah container berjalan
    if ! docker compose ps | grep -q "$PG_SERVICE_NAME"; then
        log_error "Container Postgres tidak berjalan! Coba jalankan 'docker compose up -d' dulu."
        exit 1
    fi

    # Eksekusi pg_dump dari dalam container
    # -c: Clean (DROP commands included)
    # -U: User
    docker compose exec -T $PG_SERVICE_NAME pg_dump -U $PG_USER -c $PG_DB > "$CURRENT_BACKUP_PATH/postgres_dump.sql"
    
    if [ $? -eq 0 ]; then
        log_success "Database dump tersimpan di: $CURRENT_BACKUP_PATH/postgres_dump.sql"
    else
        log_error "Gagal melakukan backup Database!"
        rm -rf "$CURRENT_BACKUP_PATH"
        exit 1
    fi

    # --- STEP B: BACKUP MINIO (FILE STORAGE) ---
    log_info "Backing up MinIO Data Lake..."
    
    if [ -d "$MINIO_DATA_DIR" ]; then
        # Mengompres folder minio_data
        tar -czf "$CURRENT_BACKUP_PATH/minio_data_backup.tar.gz" -C . "minio_data"
        log_success "MinIO data tersimpan di: $CURRENT_BACKUP_PATH/minio_data_backup.tar.gz"
    else
        log_warn "Folder $MINIO_DATA_DIR tidak ditemukan. Skip backup MinIO."
    fi

    log_success "✅ BACKUP SELESAI! ID: $TIMESTAMP"
    log_info "Lokasi: $CURRENT_BACKUP_PATH"
}

# ==============================================================================
# 2. MODUL RESTORE
# ==============================================================================
run_restore() {
    TARGET_ID=$1
    RESTORE_PATH="$BACKUP_DIR/$TARGET_ID"

    if [ -z "$TARGET_ID" ]; then
        log_error "Harap tentukan ID Backup yang mau di-restore."
        log_info "Contoh: ./manage_disaster_recovery.sh restore 20231025_120000"
        log_info "List Backup Tersedia:"
        ls -1 $BACKUP_DIR
        exit 1
    fi

    if [ ! -d "$RESTORE_PATH" ]; then
        log_error "Folder backup '$RESTORE_PATH' tidak ditemukan!"
        exit 1
    fi

    log_warn "⚠️  PERINGATAN: PROSES INI AKAN MENIMPA DATA YANG ADA!"
    read -p "Apakah Anda yakin ingin melanjutkan? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Restore dibatalkan."
        exit 1
    fi

    # --- STEP A: RESTORE POSTGRESQL ---
    SQL_FILE="$RESTORE_PATH/postgres_dump.sql"
    if [ -f "$SQL_FILE" ]; then
        log_info "Restoring PostgreSQL dari $SQL_FILE..."
        
        # Drop koneksi aktif lain jika perlu (opsional), lalu restore
        cat "$SQL_FILE" | docker compose exec -T $PG_SERVICE_NAME psql -U $PG_USER -d $PG_DB
        
        if [ $? -eq 0 ]; then
            log_success "Database berhasil dipulihkan."
        else
            log_error "Gagal me-restore database."
            exit 1
        fi
    else
        log_error "File dump SQL tidak ditemukan di folder backup."
    fi

    # --- STEP B: RESTORE MINIO ---
    MINIO_ARCHIVE="$RESTORE_PATH/minio_data_backup.tar.gz"
    if [ -f "$MINIO_ARCHIVE" ]; then
        log_info "Restoring MinIO Data..."
        
        # Hapus data lama (Opsional, agar bersih)
        rm -rf "$MINIO_DATA_DIR"
        
        # Extract archive
        tar -xzf "$MINIO_ARCHIVE" -C .
        
        log_success "MinIO Data berhasil dipulihkan."
    else
        log_warn "Arsip MinIO tidak ditemukan. Skip restore MinIO."
    fi

    log_success "✅ RESTORE SELESAI! Sistem telah kembali ke kondisi: $TARGET_ID"
}

# ==============================================================================
# MAIN MENU
# ==============================================================================
case "$1" in
    backup)
        run_backup
        ;;
    restore)
        run_restore "$2"
        ;;
    *)
        echo "Usage: $0 {backup|restore <backup_id>}"
        echo "Examples:"
        echo "  $0 backup"
        echo "  $0 restore 20240101_120000"
        exit 1
        ;;
esac
