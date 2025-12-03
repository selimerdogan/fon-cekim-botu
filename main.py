def upload_to_firestore(df):
    collection_name = "fonlar"
    
    # 1. Debug: Sütun isimlerini görelim (GitHub loglarında ne olduğunu anlamak için)
    print(f"Gelen Tablo Sütunları: {df.columns.tolist()}")
    
    # 2. Veri Temizliği: Sütun isimlerindeki gereksiz boşlukları silelim
    df.columns = df.columns.str.strip()
    
    # 3. Fon Kodunun olduğu sütunu garantiye alalım
    # Kod genelde 'Kod', 'Fon Kodu' ya da tablonun İLK sütunudur.
    # Biz işi garantiye alıp tablonun 0. indeksindeki (ilk) sütunu kod olarak kabul edelim.
    code_column_name = df.columns[0]
    print(f"Fon Kodları '{code_column_name}' sütunundan okunacak.")

    print(f"{len(df)} adet fon Firestore'a yükleniyor...")
    
    records = df.to_dict(orient='records')
    
    count = 0
    for item in records:
        # Dinamik olarak belirlediğimiz sütun ismini kullanıyoruz
        fon_kodu = item.get(code_column_name)
        
        # Fon kodu boş değilse ve string ise işlem yap
        if fon_kodu and isinstance(fon_kodu, str):
            # Firestore döküman ID'sinde "/" gibi karakterler olamaz, temizleyelim
            clean_id = fon_kodu.replace('/', '-').strip()
            
            doc_ref = db.collection(collection_name).document(clean_id)
            
            # Tarih verisini ekle
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            
            # Veriyi yaz
            doc_ref.set(item)
            count += 1
            
    print(f"İşlem Tamamlandı: {count} fon güncellendi.")
