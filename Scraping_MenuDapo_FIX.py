import time
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import datetime
import pyodbc


# Connecc ke DB MI
Driver='{SQL server}'
server = '*****'
database = '*****'
username = '*****'
password = '*****'
DBconnect = pyodbc.connect ('Driver='+Driver+';Server='+server+';Database='+database+';UID='+username+';PWD='+password)


headers = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
}

githubadapter = HTTPAdapter(max_retries=3)
session = requests.Session()
session.mount("https://dapo.kemdikbud.go.id/api", githubadapter)

#Time


def checkDB(table_name,table_type):
    cursor = DBconnect.cursor()
    # Check Table ada atau tidak
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"+table_name+"'")
    existcheck = cursor.fetchone()
    if existcheck[0] == 0:
        print("There's no table like that, Creating Table...")
        user_table_choice = table_type
        # Membuat Table
        if user_table_choice == 1 or user_table_choice == 2:
             level_skul = "dapodik_KB" if user_table_choice == 1 else "dapodik_TK"
             pd_holder = ""



        elif user_table_choice == 3:
            level_skul = "dapodik_SD"
            pd_holder ="pd_kelas1_ganjil INT, " \
                        "pd_kelas1_genap INT, " \
                        "pd_kelas2_ganjil INT, " \
                        "pd_kelas2_genap INT, " \
                        "pd_kelas3_ganjil INT, " \
                        "pd_kelas3_genap INT, " \
                        "pd_kelas4_ganjil INT, " \
                        "pd_kelas4_genap INT, " \
                        "pd_kelas5_ganjil INT, " \
                        "pd_kelas5_genap INT, " \
                        "pd_kelas6_ganjil INT, " \
                        "pd_kelas6_genap INT, "

        elif user_table_choice == 4:
            level_skul = "dapodik_SMP"

            pd_holder = "pd_kelas7_ganjil INT, " \
                        "pd_kelas7_genap INT, " \
                        "pd_kelas8_ganjil INT, " \
                        "pd_kelas8_genap INT, " \
                        "pd_kelas9_ganjil INT, " \
                        "pd_kelas9_genap INT, "

        elif user_table_choice == 5:
            level_skul = "dapodik_SMA"

            pd_holder = "pd_kelas10_ganjil INT, " \
                        "pd_kelas10_genap INT, " \
                        "pd_kelas11_ganjil INT, " \
                        "pd_kelas11_genap INT, " \
                        "pd_kelas12_ganjil INT, " \
                        "pd_kelas12_genap INT, " \
                        "merge_wilayah VARCHAR(255), "

        elif user_table_choice == 6:
            level_skul = "dapodik_SMK"

            pd_holder = "pd_kelas10_ganjil INT, " \
                        "pd_kelas10_genap INT, " \
                        "pd_kelas11_ganjil INT, " \
                        "pd_kelas11_genap INT, " \
                        "pd_kelas12_ganjil INT, " \
                        "pd_kelas12_genap INT, " \
                        "pd_kelas13_ganjil INT, " \
                        "pd_kelas13_genap INT, "

        else:
            print("Only 1 - 6, Try Again")
            checkDB()

        # Ini buat nambah 4 data Dapo di Akhir. SMK ga ada soalnya.
        if user_table_choice != 6 :
            dapo4data_holder = ", sk_pendirian_sekolah VARCHAR (255), " \
                               "tanggal_sk_pendidikan DATE, " \
                               "sk_izin_operasional VARCHAR(255), " \
                               "tanggal_sk_izin_operasional DATE)"
        else:
            dapo4data_holder = ")"

        table_skul = ("CREATE TABLE "+level_skul+" (Tahun_ajaran INT," \
                                         "nama VARCHAR(255)," \
                                         "npsn VARCHAR(255) not NULL," \
                                         "bentuk_pendidikan VARCHAR(255), " \
                                         "status_sekolah VARCHAR(255)," \
                                         "sekolah_id VARCHAR(255), " \
                                         "sekolah_id_enkrip VARCHAR(255), " \
                                         "alamat VARCHAR(255), " \
                                         "kecamatan VARCHAR(255), " \
                                         "kabupaten VARCHAR(255)," \
                                         "propinsi VARCHAR(255), " \
                                         "ptk_lakiganjil INT, " \
                                         "ptk_lakigenap INT, " \
                                         "ptk_perempuanganjil INT, " \
                                         "ptk_perempuangenap INT, " \
                                         "ptkganjil INT, " \
                                         "ptkgenap INT," \
                                         "pegawai_lakiganjil INT, " \
                                         "pegawai_lakigenap INT, " \
                                         "pegawai_perempuanganjil INT, " \
                                         "pegawai_perempuangenap INT, " \
                                         "pegawaiganjil INT, " \
                                         "pegawaigenap INT," \
                                         "guru_kelasganjil INT, " \
                                         "guru_kelasgenap INT, " \
                                         "guru_matematikaganjil INT, " \
                                         "guru_matematikagenap INT, " \
                                         "guru_bahasa_indonesiaganjil INT, " \
                                         "guru_bahasa_indonesiagenap INT, " \
                                         "guru_bahasa_inggrisganjil INT, " \
                                         "guru_bahasa_inggrisgenap INT," \
                                         "guru_sejarah_indonesiaganjil INT, " \
                                         "guru_sejarah_indonesiagenap INT, " \
                                         "guru_pknganjil INT, " \
                                         "guru_pkngenap INT," \
                                         "guru_penjaskesganjil INT, " \
                                         "guru_penjaskesgenap INT, " \
                                         "guru_agamaganjil INT, " \
                                         "guru_agamagenap INT," \
                                         "guru_seni_budayaganjil INT, " \
                                         "guru_seni_budayagenap INT," \
                                         "before_ruang_kelasganjil INT," \
                                         "before_ruang_kelasgenap INT, " \
                                         "after_ruang_kelasganjil INT," \
                                         "after_ruang_kelasgenap INT, " \
                                         "before_ruang_perpusganjil INT," \
                                         "before_ruang_perpusgenap INT, " \
                                         "after_ruang_perpusganjil INT," \
                                         "after_ruang_perpusgenap INT, " \
                                         "before_ruang_labganjil INT," \
                                         "before_ruang_labgenap INT, " \
                                         "after_ruang_labganjil INT," \
                                         "after_ruang_labgenap INT, " \
                                         "before_ruang_praktikganjil INT," \
                                         "before_ruang_praktikgenap INT, " \
                                         "after_ruang_praktikganjil INT," \
                                         "after_ruang_praktikgenap INT, " \
                                         "before_ruang_guruganjil INT," \
                                         "before_ruang_gurugenap INT, " \
                                         "after_ruang_guruganjil INT," \
                                         "after_ruang_gurugenap INT, " \
                                         "before_ruang_ibadahganjil INT," \
                                         "before_ruang_ibadahgenap INT, " \
                                         "after_ruang_ibadahganjil INT," \
                                         "after_ruang_ibadahgenap INT, " \
                                         "before_ruang_uksganjil INT," \
                                         "before_ruang_uksgenap INT, " \
                                         "after_ruang_uksganjil INT," \
                                         "after_ruang_uksgenap INT, " \
                                         "before_ruang_sirkulasiganjil INT," \
                                         "before_ruang_sirkulasigenap INT, " \
                                         "after_ruang_sirkulasiganjil INT," \
                                         "after_ruang_sirkulasigenap INT, " \
                                         "before_tempat_bermain_olahragaganjil INT, " \
                                         "before_tempat_bermain_olahragagenap INT, " \
                                         "after_tempat_bermain_olahragaganjil INT, " \
                                         "after_tempat_bermain_olahragagenap INT, " \
                                         "before_bangunanganjil INT, " \
                                         "before_bangunangenap INT, " \
                                         "after_bangunanganjil INT, " \
                                         "after_bangunangenap INT, " \
                                         "sumber_airganjil VARCHAR(255), " \
                                         "sumber_airgenap VARCHAR(255), " \
                                         "sumber_air_minumganjil VARCHAR(255), " \
                                         "sumber_air_minumgenap VARCHAR(255), " \
                                         "kecukupan_air_bersihganjil VARCHAR(255), " \
                                         "kecukupan_air_bersihgenap VARCHAR(255), " \
                                         "rombel_ganjil INT, " \
                                         "rombel_genap INT, " \
                                         "pd_ganjil INT, " \
                                         "pd_genap INT, " \
                                         "pd_laki_ganjil INT, " \
                                         "pd_laki_genap INT, " \
                                         "pd_perempuan_ganjil INT, " \
                                         "pd_perempuan_genap INT, " \
                                         +pd_holder+
                                         "sinkron_terakhir VARCHAR(255), " \
                                         "Time VARCHAR(255)" \
                                         +dapo4data_holder)

        cursor.execute(table_skul)
        cursor.close()
        print(f"table {level_skul}  is created!")

    else:
        cursor.close()
        print(f"Table {table_name} Existed")

def checkDataDB(table_name,tahun_ajaran,npsn,insert_dataECY,update_dataECY, values):
    check_data_cursor = DBconnect.cursor()
    check_data_query = (f"SELECT COUNT(nama) FROM {table_name} WHERE Tahun_ajaran = '{tahun_ajaran}' AND npsn = '{npsn}'")
    check_data_cursor.execute(check_data_query)
    checkdata = check_data_cursor.fetchone()[0] == 1
    if checkdata < 1:
        print("Data Non Existed, Inserting Data..")
        check_data_cursor.execute(insert_dataECY, values)
        DBconnect.commit()
        print(f"Data {npsn} Inserted")
    else:
        print("Data Existed!")
        check_data_cursor.execute(update_dataECY,values)
        DBconnect.commit()
        check_data_cursor.close()
        print(f"Data {npsn} Updated")

def dapodik4data(table_nama,skul_id_enkrip,tahun_ajaran):
    data4update = DBconnect.cursor()
    url = "https://dapo.kemdikbud.go.id/sekolah/" + skul_id_enkrip
    result = requests.get(url)

    doc = BeautifulSoup(result.text, "html.parser")
    nama = doc.find("h2",attrs={"class":"name"})
    if nama is None:
        print('Sekolah Tidak Ada!')
    else:
        nama_text = nama.text

        # ini error handling jika sekolah sudah tidak ada (404 Error)
        data1S = doc.find("strong", string="SK Pendirian Sekolah : ")
        data1 = data1S.next_sibling
        if data1 is None:
            print("Tidak Ada SK Pendirian Sekolah")
        else:
            sk_pendirian_skl = data1
            querry1 = f"UPDATE {table_nama} SET sk_pendirian_sekolah = ? WHERE sekolah_id_enkrip = ? AND Tahun_ajaran =?"
            data4update.execute(querry1,(sk_pendirian_skl,skul_id_enkrip,tahun_ajaran))
            data4update.commit()

        data2S = doc.find("strong", string="Tanggal SK Pendirian : ")
        data2 = data2S.next_sibling
        if data2 is None:
            print("Tidak Ada Tanggal SK Pendidikan!")
        else:
            tanggal_sk_pend = data2
            data4update.execute(f"UPDATE {table_nama} SET tanggal_sk_pendidikan = '{tanggal_sk_pend}' WHERE sekolah_id_enkrip = '{skul_id_enkrip}' AND Tahun_ajaran ='{tahun_ajaran}'")
            data4update.commit()


        data3S = doc.find("strong", string="SK Izin Operasional : ")
        data3 = data3S.next_sibling
        if data3 is None:
            print("Tidak Ada SK Izin Operasional")
        else:
            sk_izin_op = data3
            querry2 = f"UPDATE {table_nama} SET sk_izin_operasional = ? WHERE sekolah_id_enkrip = ? AND Tahun_ajaran = ?"
            data4update.execute(querry2,(sk_izin_op,skul_id_enkrip,tahun_ajaran))
            data4update.commit()


        data4S = doc.find("strong", string="Tanggal SK Izin Operasional : ")
        data4 = data4S.next_sibling
        if data4 is None:
            print("Tidak Ada Tanggal SK Izin Operasional")
        else:
            tanggal_sk_izin_op = data4
            data4update.execute(f"UPDATE {table_nama} SET tanggal_sk_izin_operasional = '{tanggal_sk_izin_op}' WHERE sekolah_id_enkrip = '{skul_id_enkrip}'AND Tahun_ajaran ='{tahun_ajaran}'")
            data4update.commit()

        print("Dapodik 4 data for",nama_text,"is Done!")

def scrapingECY(table_name,level,tahun_ajaran,progres_view,provStart,provEnd):
    ctr = 1
    #Menentukan level dan generate Link masing - masing level
    if level == "kb" or level == "tk":
        progres_level = "paud"
        progess_level3 = "SpPaud"
        view_ecy = "&view=kb" if level == "kb" else "&view=tk"
    else:
        progres_level = level
        progess_level3 = "SP-"+"".join(level) #Ini agar dapat Concanete Level yang merupakan Set
        view_ecy = ""

    # Loop Buat Dapetin Kode_Provinsi
    for i in range(provStart, provEnd):
        kode_prov = str(i).zfill(2)

        # KOTA / KABUPATEN
        url_kota = f"https://dapo.kemdikbud.go.id/rekap/progres-{progres_level}?id_level_wilayah=1&kode_wilayah={kode_prov}0000&semester_id={progres_view}{view_ecy}"
        result_kota = session.get(url_kota,headers=headers)
        if result_kota.status_code == 200:
            data_kota = result_kota.json()
            try:
                for kota in data_kota:
                    nama_kota = kota["nama"]
                    kode_kota = kota["kode_wilayah"].strip(" ")
                    print(nama_kota,kode_kota)

                    # KECAMATAN
                    url = f"https://dapo.kemdikbud.go.id/rekap/progres-{progres_level}?id_level_wilayah=2&kode_wilayah={kode_kota}&semester_id={progres_view}{view_ecy}"
                    result = session.get(url, headers=headers)
                    if result.status_code == 200:
                        data = result.json()
                        try:
                            for kecamatan in data:
                                nama_kecamatan = kecamatan["nama"]
                                kode_kec = kecamatan["kode_wilayah"].strip(" ")
                                print(nama_kecamatan,kode_kec)

                                # SEKOLAH
                                url_kec = f"https://dapo.kemdikbud.go.id/rekap/progres{progess_level3}?id_level_wilayah=3&kode_wilayah={kode_kec}&semester_id={progres_view}{view_ecy}"
                                result_kec = session.get(url_kec,headers=headers)
                                if result_kec.status_code == 200:
                                    try:
                                        data_sekolah = result_kec.json()
                                        for sekolah in data_sekolah:
                                            nama_skul = sekolah["nama"]
                                            npsn = sekolah["npsn"]
                                            kecamatan = sekolah["induk_kecamatan"]
                                            kabkot = sekolah["induk_kabupaten"]
                                            provinsi = sekolah["induk_provinsi"]
                                            bentuk_pend = sekolah["bentuk_pendidikan"]
                                            status_sekolah = sekolah["status_sekolah"]
                                            sinkron_terakhir = sekolah["sinkron_terakhir"]
                                            skul_id = sekolah["sekolah_id"].strip(" ")
                                            skul_id_enkrip = sekolah["sekolah_id_enkrip"].strip(" ")
                                            merge_wilayah = provinsi +"; "+kabkot+"; "+kecamatan
                                            print("===========================")
                                            print("Sekolah Ke. "+str(ctr)+" : "+nama_skul,"(",npsn,") CP : "+kode_prov)

                                            # Pausing code for faster long term scrape
                                            if ctr % 1000 == 0:
                                                print("Pausing Code 5 Minutes")
                                                time.sleep(300)
                                                print("Resuming Code")
                                                ctr+=1
                                            else:
                                                ctr += 1

                                            # Alamat Sekolah
                                            url_alamat = f"https://dapo.kemdikbud.go.id/api/getHasilPencarian?keyword={npsn}"
                                            result_url_alamat = session.get(url_alamat,headers=headers)
                                            if result_url_alamat.status_code == 200:
                                                json_alamat = result_url_alamat.json()
                                                if len(json_alamat) > 0:
                                                    for data_alamat in json_alamat:
                                                        alamat = data_alamat["alamat_jalan"]
                                                else:
                                                    alamat = "NULL"
                                            else:
                                                print("Request failed with status code:", result_url_alamat.status_code)
                                                print(result_url_alamat.content)


                                            # SEKOLAH DETAIL
                                            # SEMESTER GANJIL
                                            url_skulGANJIL = f"https://dapo.kemdikbud.go.id/rekap/sekolahDetail?semester_id={tahun_ajaran}1&sekolah_id={skul_id_enkrip}"
                                            result_skulGANJIL = session.get(url_skulGANJIL,headers=headers)
                                            if result_skulGANJIL.status_code == 200:
                                                sekolah_detGANJIL = result_skulGANJIL.json()
                                                if len(sekolah_detGANJIL) > 0:
                                                    for detailGANJIL in sekolah_detGANJIL:
                                                        rombel_ganjil = detailGANJIL["rombel"]
                                                        ptk_lakiganjil = detailGANJIL["ptk_laki"]
                                                        ptk_perempuanganjil = detailGANJIL["ptk_perempuan"]
                                                        ptk_ganjil = detailGANJIL["ptk"]
                                                        pegawai_lakiganjil = detailGANJIL["pegawai_laki"]
                                                        pegawai_perempuanganjil = detailGANJIL["pegawai_perempuan"]
                                                        pegawai_ganjil = detailGANJIL["pegawai"]
                                                        guru_kelasganjil = detailGANJIL["guru_kelas"]
                                                        guru_matematikaganjil = detailGANJIL["guru_matematika"]
                                                        guru_bindoganjil = detailGANJIL["guru_bahasa_indonesia"]
                                                        guru_binggrisganjil = detailGANJIL["guru_bahasa_inggris"]
                                                        guru_sejarahganjil = detailGANJIL["guru_sejarah_indonesia"]
                                                        guru_pknganjil = detailGANJIL["guru_pkn"]
                                                        guru_penjaskesganjil = detailGANJIL["guru_penjaskes"]
                                                        guru_agamaganjil = detailGANJIL["guru_agama_budi_pekerti"]
                                                        guru_seniganjil = detailGANJIL["guru_seni_budaya"]
                                                        pd_lakiganjil = detailGANJIL["pd_laki"]
                                                        pd_perempuanganjil = detailGANJIL["pd_perempuan"]
                                                        pd_ganjil = detailGANJIL["pd"]
                                                        if level == "sd" :
                                                            pd_kelas1ganjil = (detailGANJIL.get("pd_kelas_1_laki", 0)) + (detailGANJIL.get("pd_kelas_1_perempuan", 0))
                                                            pd_kelas2ganjil = (detailGANJIL.get("pd_kelas_2_laki", 0)) + (detailGANJIL.get("pd_kelas_2_perempuan", 0))
                                                            pd_kelas3ganjil = (detailGANJIL.get("pd_kelas_3_laki", 0)) + (detailGANJIL.get("pd_kelas_3_perempuan", 0))
                                                            pd_kelas4ganjil = (detailGANJIL.get("pd_kelas_4_laki", 0)) + (detailGANJIL.get("pd_kelas_4_perempuan", 0))
                                                            pd_kelas5ganjil = (detailGANJIL.get("pd_kelas_5_laki", 0)) + (detailGANJIL.get("pd_kelas_5_perempuan", 0))
                                                            pd_kelas6ganjil = (detailGANJIL.get("pd_kelas_6_laki", 0)) + (detailGANJIL.get("pd_kelas_6_perempuan", 0))
                                                        elif level == "smp" :
                                                            pd_kelas7ganjil = (detailGANJIL.get("pd_kelas_7_laki", 0)) + (detailGANJIL.get("pd_kelas_7_perempuan", 0))
                                                            pd_kelas8ganjil = (detailGANJIL.get("pd_kelas_8_laki", 0)) + (detailGANJIL.get("pd_kelas_8_perempuan", 0))
                                                            pd_kelas9ganjil = (detailGANJIL.get("pd_kelas_9_laki", 0)) + (detailGANJIL.get("pd_kelas_9_perempuan", 0))
                                                        elif level == "sma" :
                                                            pd_kelas10ganjil = (detailGANJIL.get("pd_kelas_10_laki", 0) or 0) + (detailGANJIL.get("pd_kelas_10_perempuan", 0) or 0)
                                                            pd_kelas11ganjil = (detailGANJIL.get("pd_kelas_11_laki", 0) or 0) + (detailGANJIL.get("pd_kelas_11_perempuan", 0) or 0)
                                                            pd_kelas12ganjil = (detailGANJIL.get("pd_kelas_12_laki", 0) or 0) + (detailGANJIL.get("pd_kelas_12_perempuan", 0) or 0)
                                                        elif level == "smk" :
                                                            pd_kelas10ganjil = (detailGANJIL.get("pd_kelas_10_laki", 0)) + (detailGANJIL.get("pd_kelas_10_perempuan", 0))
                                                            pd_kelas11ganjil = (detailGANJIL.get("pd_kelas_11_laki", 0)) + (detailGANJIL.get("pd_kelas_11_perempuan", 0))
                                                            pd_kelas12ganjil = (detailGANJIL.get("pd_kelas_12_laki", 0)) + (detailGANJIL.get("pd_kelas_12_perempuan", 0))
                                                            pd_kelas13ganjil = (detailGANJIL.get("pd_kelas_13_laki", 0)) + (detailGANJIL.get("pd_kelas_13_perempuan", 0))
                                                        before_RKelas_ganjil = detailGANJIL["before_ruang_kelas"]
                                                        after_Rkelas_ganjil = detailGANJIL["after_ruang_kelas"]
                                                        before_RPerpus_ganjil = detailGANJIL["before_ruang_perpus"]
                                                        after_RPerpus_ganjil = detailGANJIL["after_ruang_perpus"]
                                                        before_RLab_ganjil = detailGANJIL["before_ruang_lab"]
                                                        after_RLab_ganjil = detailGANJIL["after_ruang_lab"]
                                                        before_RPraktik_ganjil = detailGANJIL["before_ruang_praktik"]
                                                        after_RPraktik_ganjil = detailGANJIL["after_ruang_praktik"]
                                                        before_RGuru_ganjil = detailGANJIL["before_ruang_guru"]
                                                        after_RGuru_ganjil = detailGANJIL["after_ruang_guru"]
                                                        before_RIbadah_ganjil = detailGANJIL["before_ruang_ibadah"]
                                                        after_RIbadah_ganjil = detailGANJIL["after_ruang_ibadah"]
                                                        before_RUKS_ganjil = detailGANJIL["before_ruang_uks"]
                                                        after_RUKS_ganjil = detailGANJIL["after_ruang_uks"]
                                                        before_RSirkulasi_ganjil = detailGANJIL["before_ruang_sirkulasi"]
                                                        after_RSirkulasi_ganjil = detailGANJIL["after_ruang_sirkulasi"]
                                                        before_TMainOlahraga_ganjil = detailGANJIL["before_tempat_bermain_olahraga"]
                                                        after_TMainOlahraga_ganjil = detailGANJIL["after_tempat_bermain_olahraga"]
                                                        before_Bangunan_ganjil = detailGANJIL["before_bangunan"]
                                                        after_Bangunan_ganjil = detailGANJIL["after_bangunan"]
                                                        sumberair_ganjil = detailGANJIL["sumber_air"]
                                                        sumberair_minum_ganjil = detailGANJIL["sumber_air_minum"]
                                                        kec_airbersih_ganjil = detailGANJIL["kecukupan_air_bersih"]
                                                        print("Student Numbers GANJIL = ", pd_ganjil)

                                                else:
                                                        rombel_ganjil=ptk_lakiganjil=ptk_perempuanganjil=ptk_ganjil=pegawai_lakiganjil=pegawai_perempuanganjil= \
                                                        pegawai_ganjil=guru_kelasganjil=guru_matematikaganjil=guru_bindoganjil=guru_binggrisganjil=guru_sejarahganjil= \
                                                        guru_pknganjil=guru_penjaskesganjil=guru_agamaganjil=guru_seniganjil=pd_lakiganjil=pd_perempuanganjil= \
                                                        pd_ganjil=before_RKelas_ganjil=after_Rkelas_ganjil=before_RPerpus_ganjil=after_RPerpus_ganjil=before_RLab_ganjil= \
                                                        after_RLab_ganjil=before_RPraktik_ganjil=after_RPraktik_ganjil=before_RGuru_ganjil=after_RGuru_ganjil= \
                                                        before_RIbadah_ganjil=after_RIbadah_ganjil=before_RUKS_ganjil=after_RUKS_ganjil=before_RSirkulasi_ganjil= \
                                                        after_RSirkulasi_ganjil=before_TMainOlahraga_ganjil=after_TMainOlahraga_ganjil=before_Bangunan_ganjil= \
                                                        after_Bangunan_ganjil=sumberair_ganjil=sumberair_minum_ganjil=kec_airbersih_ganjil= None

                                                        if level == "sd":
                                                            pd_kelas1ganjil=pd_kelas2ganjil=pd_kelas3ganjil=pd_kelas4ganjil=pd_kelas5ganjil=pd_kelas6ganjil= None
                                                        elif level == "smp":
                                                            pd_kelas7ganjil=pd_kelas8ganjil=pd_kelas9ganjil= None
                                                        elif level == "sma":
                                                            pd_kelas10ganjil=pd_kelas11ganjil=pd_kelas12ganjil= None
                                                        elif level == "smk":
                                                            pd_kelas10ganjil = pd_kelas11ganjil = pd_kelas12ganjil=pd_kelas13ganjil = None



                                            else:
                                                print("Request failed with status code:", result_skulGANJIL.status_code)
                                                print(result_skulGANJIL.content)

                                            # SEMESTER GENAP
                                            url_skulGENAP = f"https://dapo.kemdikbud.go.id/rekap/sekolahDetail?semester_id={tahun_ajaran}2&sekolah_id={skul_id_enkrip}"
                                            result_skulGENAP = session.get(url_skulGENAP, headers=headers)
                                            if result_skulGENAP.status_code == 200:
                                                sekolah_detGENAP = result_skulGENAP.json()
                                                if len(sekolah_detGENAP) > 0:
                                                    for detailGENAP in sekolah_detGENAP:
                                                        rombel_genap = detailGENAP["rombel"]
                                                        ptk_lakigenap = detailGENAP["ptk_laki"]
                                                        ptk_perempuangenap = detailGENAP["ptk_perempuan"]
                                                        ptk_genap = detailGENAP["ptk"]
                                                        pegawai_lakigenap = detailGENAP["pegawai_laki"]
                                                        pegawai_perempuangenap = detailGENAP["pegawai_perempuan"]
                                                        pegawai_genap = detailGENAP["pegawai"]
                                                        guru_kelasgenap = detailGENAP["guru_kelas"]
                                                        guru_matematikagenap = detailGENAP["guru_matematika"]
                                                        guru_bindogenap = detailGENAP["guru_bahasa_indonesia"]
                                                        guru_binggrisgenap = detailGENAP["guru_bahasa_inggris"]
                                                        guru_sejarahgenap = detailGENAP["guru_sejarah_indonesia"]
                                                        guru_pkngenap = detailGENAP["guru_pkn"]
                                                        guru_penjaskesgenap = detailGENAP["guru_penjaskes"]
                                                        guru_agamagenap = detailGENAP["guru_agama_budi_pekerti"]
                                                        guru_senigenap = detailGENAP["guru_seni_budaya"]
                                                        pd_lakigenap = detailGENAP["pd_laki"]
                                                        pd_perempuangenap = detailGENAP["pd_perempuan"]
                                                        pd_genap = detailGENAP["pd"]
                                                        if level == "sd" :
                                                            pd_kelas1genap = (detailGENAP.get("pd_kelas_1_laki", 0)) + (detailGENAP.get("pd_kelas_1_perempuan", 0))
                                                            pd_kelas2genap = (detailGENAP.get("pd_kelas_2_laki", 0)) + (detailGENAP.get("pd_kelas_2_perempuan", 0))
                                                            pd_kelas3genap = (detailGENAP.get("pd_kelas_3_laki", 0)) + (detailGENAP.get("pd_kelas_3_perempuan", 0))
                                                            pd_kelas4genap = (detailGENAP.get("pd_kelas_4_laki", 0)) + (detailGENAP.get("pd_kelas_4_perempuan", 0))
                                                            pd_kelas5genap = (detailGENAP.get("pd_kelas_5_laki", 0)) + (detailGENAP.get("pd_kelas_5_perempuan", 0))
                                                            pd_kelas6genap = (detailGENAP.get("pd_kelas_6_laki", 0)) + (detailGENAP.get("pd_kelas_6_perempuan", 0))
                                                        elif level == "smp" :
                                                            pd_kelas7genap = (detailGENAP.get("pd_kelas_7_laki", 0)) + (detailGENAP.get("pd_kelas_7_perempuan", 0))
                                                            pd_kelas8genap = (detailGENAP.get("pd_kelas_8_laki", 0)) + (detailGENAP.get("pd_kelas_8_perempuan", 0))
                                                            pd_kelas9genap = (detailGENAP.get("pd_kelas_9_laki", 0)) + (detailGENAP.get("pd_kelas_9_perempuan", 0))
                                                        elif level == "sma" :
                                                            pd_kelas10genap = (detailGENAP.get("pd_kelas_10_laki", 0) or 0) + (detailGENAP.get("pd_kelas_10_perempuan", 0) or 0)
                                                            pd_kelas11genap = (detailGENAP.get("pd_kelas_11_laki", 0) or 0) + (detailGENAP.get("pd_kelas_11_perempuan", 0) or 0)
                                                            pd_kelas12genap = (detailGENAP.get("pd_kelas_12_laki", 0) or 0) + (detailGENAP.get("pd_kelas_12_perempuan", 0) or 0)
                                                        elif level == "smk" :
                                                            pd_kelas10genap = (detailGENAP.get("pd_kelas_10_laki", 0)) + (detailGENAP.get("pd_kelas_10_perempuan", 0))
                                                            pd_kelas11genap = (detailGENAP.get("pd_kelas_11_laki", 0)) + (detailGENAP.get("pd_kelas_11_perempuan", 0))
                                                            pd_kelas12genap = (detailGENAP.get("pd_kelas_12_laki", 0)) + (detailGENAP.get("pd_kelas_12_perempuan", 0))
                                                            pd_kelas13genap = (detailGENAP.get("pd_kelas_13_laki", 0)) + (detailGENAP.get("pd_kelas_13_perempuan", 0))
                                                        before_RKelas_genap = detailGENAP["before_ruang_kelas"]
                                                        after_Rkelas_genap = detailGENAP["after_ruang_kelas"]
                                                        before_RPerpus_genap = detailGENAP["before_ruang_perpus"]
                                                        after_RPerpus_genap = detailGENAP["after_ruang_perpus"]
                                                        before_RLab_genap = detailGENAP["before_ruang_lab"]
                                                        after_RLab_genap = detailGENAP["after_ruang_lab"]
                                                        before_RPraktik_genap = detailGENAP["before_ruang_praktik"]
                                                        after_RPraktik_genap = detailGENAP["after_ruang_praktik"]
                                                        before_RGuru_genap = detailGENAP["before_ruang_guru"]
                                                        after_RGuru_genap = detailGENAP["after_ruang_guru"]
                                                        before_RIbadah_genap = detailGENAP["before_ruang_ibadah"]
                                                        after_RIbadah_genap = detailGENAP["after_ruang_ibadah"]
                                                        before_RUKS_genap = detailGENAP["before_ruang_uks"]
                                                        after_RUKS_genap = detailGENAP["after_ruang_uks"]
                                                        before_RSirkulasi_genap = detailGENAP["before_ruang_sirkulasi"]
                                                        after_RSirkulasi_genap = detailGENAP["after_ruang_sirkulasi"]
                                                        before_TMainOlahraga_genap = detailGENAP["before_tempat_bermain_olahraga"]
                                                        after_TMainOlahraga_genap = detailGENAP["after_tempat_bermain_olahraga"]
                                                        before_Bangunan_genap = detailGENAP["before_bangunan"]
                                                        after_Bangunan_genap = detailGENAP["after_bangunan"]
                                                        sumberair_genap = detailGENAP["sumber_air"]
                                                        sumberair_minum_genap = detailGENAP["sumber_air_minum"]
                                                        kec_airbersih_genap = detailGENAP["kecukupan_air_bersih"]
                                                        print("Student Numbers GENAP = ", pd_genap)

                                                else:
                                                    rombel_genap = ptk_lakigenap = ptk_perempuangenap = ptk_genap = pegawai_lakigenap = pegawai_perempuangenap = \
                                                    pegawai_genap = guru_kelasgenap = guru_matematikagenap = guru_bindogenap = guru_binggrisgenap = guru_sejarahgenap = \
                                                    guru_pkngenap = guru_penjaskesgenap = guru_agamagenap = guru_senigenap = pd_lakigenap = pd_perempuangenap = \
                                                    pd_genap = before_RKelas_genap = after_Rkelas_genap = before_RPerpus_genap = after_RPerpus_genap = before_RLab_genap = \
                                                    after_RLab_genap = before_RPraktik_genap = after_RPraktik_genap = before_RGuru_genap = after_RGuru_genap = \
                                                    before_RIbadah_genap = after_RIbadah_genap = before_RUKS_genap = after_RUKS_genap = before_RSirkulasi_genap = \
                                                    after_RSirkulasi_genap = before_TMainOlahraga_genap = after_TMainOlahraga_genap = before_Bangunan_genap = \
                                                    after_Bangunan_genap = sumberair_genap = sumberair_minum_genap = kec_airbersih_genap = None

                                                    if level == "sd":
                                                        pd_kelas1genap = pd_kelas2genap = pd_kelas3genap = pd_kelas4genap = pd_kelas5genap = pd_kelas6genap = None
                                                    elif level == "smp":
                                                        pd_kelas7genap = pd_kelas8genap = pd_kelas9genap = None
                                                    elif level == "sma":
                                                        pd_kelas10genap = pd_kelas11genap = pd_kelas12genap = None
                                                    elif level == "smk":
                                                        pd_kelas10genap = pd_kelas11genap = pd_kelas12genap = pd_kelas13genap = None
                                                         
                                            else:
                                                print("Request failed with status code:",
                                                      result_skulGENAP.status_code)
                                                print(result_skulGENAP.content)

                                            input_time = datetime.datetime.now().strftime("%Y-%m-%d:%H-%M-%S")

                                            if level == "sd":
                                                colomadd = "pd_kelas1_ganjil,pd_kelas1_genap,pd_kelas2_ganjil,pd_kelas2_genap," \
                                                           "pd_kelas3_ganjil,pd_kelas3_genap,pd_kelas4_ganjil,pd_kelas4_genap," \
                                                           "pd_kelas5_ganjil,pd_kelas5_genap,pd_kelas6_ganjil,pd_kelas6_genap,"
                                                valueadd = "?,?,?,?,?,?,?,?,?,?,?,?,"


                                                updateadd = "pd_kelas1_ganjil=?,pd_kelas1_genap=?,pd_kelas2_ganjil=?,pd_kelas2_genap=?," \
                                                            "pd_kelas3_ganjil=?,pd_kelas3_genap=?,pd_kelas4_ganjil=?,pd_kelas4_genap=?," \
                                                            "pd_kelas5_ganjil=?,pd_kelas5_genap=?,pd_kelas6_ganjil=?,pd_kelas6_genap=?,"

                                            elif level == "smp":
                                                colomadd = "pd_kelas7_ganjil,pd_kelas7_genap,pd_kelas8_ganjil,pd_kelas8_genap," \
                                                           "pd_kelas9_ganjil,pd_kelas9_genap,"
                                                valueadd = "?,?,?,?,?,?,"

                                                updateadd = "pd_kelas7_ganjil=?,pd_kelas7_genap=?,pd_kelas8_ganjil=?,pd_kelas8_genap=?," \
                                                            "pd_kelas9_ganjil=?,pd_kelas9_genap=?,"

                                            elif level == "sma":
                                                colomadd = "pd_kelas10_ganjil,pd_kelas10_genap,pd_kelas11_ganjil,pd_kelas11_genap," \
                                                           "pd_kelas12_ganjil,pd_kelas12_genap,merge_wilayah,"
                                                valueadd = "?,?,?,?,?,?,?,"

                                                updateadd = "pd_kelas10_ganjil=?,pd_kelas10_genap=?,pd_kelas11_ganjil=?,pd_kelas11_genap=?," \
                                                            "pd_kelas12_ganjil=?,pd_kelas12_genap=?,merge_wilayah=?,"

                                            elif level == "smk":
                                                colomadd = "pd_kelas10_ganjil,pd_kelas10_genap,pd_kelas11_ganjil,pd_kelas11_genap," \
                                                           "pd_kelas12_ganjil,pd_kelas12_genap,pd_kelas13_ganjil,pd_kelas13_genap,"
                                                valueadd = "?,?,?,?,?,?,?,?,"

                                                updateadd = "pd_kelas10_ganjil=?,pd_kelas10_genap=?,pd_kelas11_ganjil=?,pd_kelas11_genap=?," \
                                                            "pd_kelas12_ganjil=?,pd_kelas12_genap=?,pd_kelas13_ganjil=?,pd_kelas13_genap=?,"

                                            else:
                                                colomadd = valueadd = updateadd=""


                                            insert_dataECY = (f"INSERT INTO {table_name} (Tahun_ajaran, \
                                                                npsn, \
                                                                nama, \
                                                                bentuk_pendidikan, \
                                                                status_sekolah, \
                                                                sekolah_id, \
                                                                sekolah_id_enkrip, \
                                                                alamat, \
                                                                kecamatan, \
                                                                kabupaten, \
                                                                propinsi, \
                                                                ptk_lakiganjil, \
                                                                ptk_lakigenap, \
                                                                ptk_perempuanganjil, \
                                                                ptk_perempuangenap, \
                                                                ptkganjil, \
                                                                ptkgenap, \
                                                                pegawai_lakiganjil, \
                                                                pegawai_lakigenap, \
                                                                pegawai_perempuanganjil, \
                                                                pegawai_perempuangenap, \
                                                                pegawaiganjil, \
                                                                pegawaigenap, \
                                                                guru_kelasganjil, \
                                                                guru_kelasgenap, \
                                                                guru_matematikaganjil, \
                                                                guru_matematikagenap, \
                                                                guru_bahasa_indonesiaganjil, \
                                                                guru_bahasa_indonesiagenap, \
                                                                guru_bahasa_inggrisganjil, \
                                                                guru_bahasa_inggrisgenap, \
                                                                guru_sejarah_indonesiaganjil, \
                                                                guru_sejarah_indonesiagenap, \
                                                                guru_pknganjil, \
                                                                guru_pkngenap, \
                                                                guru_penjaskesganjil, \
                                                                guru_penjaskesgenap, \
                                                                guru_agamaganjil, \
                                                                guru_agamagenap, \
                                                                guru_seni_budayaganjil, \
                                                                guru_seni_budayagenap, \
                                                                before_ruang_kelasganjil, \
                                                                before_ruang_kelasgenap, \
                                                                after_ruang_kelasganjil, \
                                                                after_ruang_kelasgenap, \
                                                                before_ruang_perpusganjil, \
                                                                before_ruang_perpusgenap, \
                                                                after_ruang_perpusganjil, \
                                                                after_ruang_perpusgenap, \
                                                                before_ruang_labganjil, \
                                                                before_ruang_labgenap, \
                                                                after_ruang_labganjil, \
                                                                after_ruang_labgenap, \
                                                                before_ruang_praktikganjil, \
                                                                before_ruang_praktikgenap, \
                                                                after_ruang_praktikganjil, \
                                                                after_ruang_praktikgenap, \
                                                                before_ruang_guruganjil, \
                                                                before_ruang_gurugenap, \
                                                                after_ruang_guruganjil, \
                                                                after_ruang_gurugenap, \
                                                                before_ruang_ibadahganjil, \
                                                                before_ruang_ibadahgenap, \
                                                                after_ruang_ibadahganjil, \
                                                                after_ruang_ibadahgenap, \
                                                                before_ruang_uksganjil, \
                                                                before_ruang_uksgenap, \
                                                                after_ruang_uksganjil, \
                                                                after_ruang_uksgenap, \
                                                                before_ruang_sirkulasiganjil, \
                                                                before_ruang_sirkulasigenap, \
                                                                after_ruang_sirkulasiganjil, \
                                                                after_ruang_sirkulasigenap, \
                                                                before_tempat_bermain_olahragaganjil, \
                                                                before_tempat_bermain_olahragagenap, \
                                                                after_tempat_bermain_olahragaganjil, \
                                                                after_tempat_bermain_olahragagenap, \
                                                                before_bangunanganjil, \
                                                                before_bangunangenap, \
                                                                after_bangunanganjil, \
                                                                after_bangunangenap, \
                                                                sumber_airganjil, \
                                                                sumber_airgenap, \
                                                                sumber_air_minumganjil, \
                                                                sumber_air_minumgenap, \
                                                                kecukupan_air_bersihganjil, \
                                                                kecukupan_air_bersihgenap, \
                                                                rombel_ganjil, \
                                                                rombel_genap, \
                                                                pd_ganjil, \
                                                                pd_genap, \
                                                                pd_laki_ganjil, \
                                                                pd_laki_genap, \
                                                                pd_perempuan_ganjil, \
                                                                pd_perempuan_genap, \
                                                                {colomadd} \
                                                                sinkron_terakhir, \
                                                                Time)\
                                                                VALUES ({valueadd}?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                                                                 +"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                                                                )
                                                
                                            update_dataECY = (f"UPDATE {table_name} SET Tahun_ajaran=?, \
                                                                npsn=?, \
                                                                nama=?, \
                                                                bentuk_pendidikan=?, \
                                                                status_sekolah=?, \
                                                                sekolah_id=?, \
                                                                sekolah_id_enkrip=?, \
                                                                alamat=?, \
                                                                kecamatan=?, \
                                                                kabupaten=?, \
                                                                propinsi=?, \
                                                                ptk_lakiganjil=?, \
                                                                ptk_lakigenap=?, \
                                                                ptk_perempuanganjil=?, \
                                                                ptk_perempuangenap=?, \
                                                                ptkganjil=?, \
                                                                ptkgenap=?, \
                                                                pegawai_lakiganjil=?, \
                                                                pegawai_lakigenap=?, \
                                                                pegawai_perempuanganjil=?, \
                                                                pegawai_perempuangenap=?, \
                                                                pegawaiganjil=?, \
                                                                pegawaigenap=?, \
                                                                guru_kelasganjil=?, \
                                                                guru_kelasgenap=?, \
                                                                guru_matematikaganjil=?, \
                                                                guru_matematikagenap=?, \
                                                                guru_bahasa_indonesiaganjil=?, \
                                                                guru_bahasa_indonesiagenap=?, \
                                                                guru_bahasa_inggrisganjil=?, \
                                                                guru_bahasa_inggrisgenap=?, \
                                                                guru_sejarah_indonesiaganjil=?, \
                                                                guru_sejarah_indonesiagenap=?, \
                                                                guru_pknganjil=?, \
                                                                guru_pkngenap=?, \
                                                                guru_penjaskesganjil=?, \
                                                                guru_penjaskesgenap=?, \
                                                                guru_agamaganjil=?, \
                                                                guru_agamagenap=?, \
                                                                guru_seni_budayaganjil=?, \
                                                                guru_seni_budayagenap=?, \
                                                                before_ruang_kelasganjil=?, \
                                                                before_ruang_kelasgenap=?, \
                                                                after_ruang_kelasganjil=?, \
                                                                after_ruang_kelasgenap=?, \
                                                                before_ruang_perpusganjil=?, \
                                                                before_ruang_perpusgenap=?, \
                                                                after_ruang_perpusganjil=?, \
                                                                after_ruang_perpusgenap=?, \
                                                                before_ruang_labganjil=?, \
                                                                before_ruang_labgenap=?, \
                                                                after_ruang_labganjil=?, \
                                                                after_ruang_labgenap=?, \
                                                                before_ruang_praktikganjil=?, \
                                                                before_ruang_praktikgenap=?, \
                                                                after_ruang_praktikganjil=?, \
                                                                after_ruang_praktikgenap=?, \
                                                                before_ruang_guruganjil=?, \
                                                                before_ruang_gurugenap=?, \
                                                                after_ruang_guruganjil=?, \
                                                                after_ruang_gurugenap=?, \
                                                                before_ruang_ibadahganjil=?, \
                                                                before_ruang_ibadahgenap=?, \
                                                                after_ruang_ibadahganjil=?, \
                                                                after_ruang_ibadahgenap=?, \
                                                                before_ruang_uksganjil=?, \
                                                                before_ruang_uksgenap=?, \
                                                                after_ruang_uksganjil=?, \
                                                                after_ruang_uksgenap=?, \
                                                                before_ruang_sirkulasiganjil=?, \
                                                                before_ruang_sirkulasigenap=?, \
                                                                after_ruang_sirkulasiganjil=?, \
                                                                after_ruang_sirkulasigenap=?, \
                                                                before_tempat_bermain_olahragaganjil=?, \
                                                                before_tempat_bermain_olahragagenap=?, \
                                                                after_tempat_bermain_olahragaganjil=?, \
                                                                after_tempat_bermain_olahragagenap=?, \
                                                                before_bangunanganjil=?, \
                                                                before_bangunangenap=?, \
                                                                after_bangunanganjil=?, \
                                                                after_bangunangenap=?, \
                                                                sumber_airganjil=?, \
                                                                sumber_airgenap=?, \
                                                                sumber_air_minumganjil=?, \
                                                                sumber_air_minumgenap=?, \
                                                                kecukupan_air_bersihganjil=?, \
                                                                kecukupan_air_bersihgenap=?, \
                                                                rombel_ganjil=?, \
                                                                rombel_genap=?, \
                                                                pd_ganjil=?, \
                                                                pd_genap=?, \
                                                                pd_laki_ganjil=?, \
                                                                pd_laki_genap=?, \
                                                                pd_perempuan_ganjil=?, \
                                                                pd_perempuan_genap=?, \
                                                                {updateadd} \
                                                                sinkron_terakhir=?, \
                                                                Time=? \
                                                                WHERE npsn = '{npsn}' AND Tahun_ajaran = '{tahun_ajaran}'"
                                            )

                                            if level == "sd":
                                                values = (tahun_ajaran, npsn, nama_skul, bentuk_pend, status_sekolah, skul_id, skul_id_enkrip, alamat, kecamatan, kabkot,
                                                          provinsi, ptk_lakiganjil, ptk_lakigenap, ptk_perempuanganjil, ptk_perempuangenap, ptk_ganjil, ptk_genap, pegawai_lakiganjil,
                                                          pegawai_lakigenap, pegawai_perempuanganjil, pegawai_perempuangenap, pegawai_ganjil, pegawai_genap, guru_kelasganjil, guru_kelasgenap,
                                                          guru_matematikaganjil, guru_matematikagenap, guru_bindoganjil, guru_bindogenap, guru_binggrisganjil, guru_binggrisgenap,
                                                          guru_sejarahganjil, guru_sejarahgenap, guru_pknganjil, guru_pkngenap, guru_penjaskesganjil, guru_penjaskesgenap, guru_agamaganjil,
                                                          guru_agamagenap, guru_seniganjil, guru_senigenap, before_RKelas_ganjil, before_RKelas_genap, after_Rkelas_ganjil, after_Rkelas_genap,
                                                          before_RPerpus_ganjil, before_RPerpus_genap, after_RPerpus_ganjil, after_RPerpus_genap, before_RLab_ganjil, before_RLab_genap,
                                                          after_RLab_ganjil, after_RLab_genap, before_RPraktik_ganjil, before_RPraktik_genap, after_RPraktik_ganjil, after_RPraktik_genap,
                                                          before_RGuru_ganjil, before_RGuru_genap, after_RGuru_ganjil, after_RGuru_genap, before_RIbadah_ganjil, before_RIbadah_genap,
                                                          after_RIbadah_ganjil, after_RIbadah_genap, before_RUKS_ganjil, before_RUKS_genap, after_RUKS_ganjil, after_RUKS_genap,
                                                          before_RSirkulasi_ganjil, before_RSirkulasi_genap, after_RSirkulasi_ganjil, after_RSirkulasi_genap, before_TMainOlahraga_ganjil,
                                                          before_TMainOlahraga_genap, after_TMainOlahraga_ganjil, after_TMainOlahraga_genap, before_Bangunan_ganjil, before_Bangunan_genap,
                                                          after_Bangunan_ganjil, after_Bangunan_genap, sumberair_ganjil, sumberair_genap, sumberair_minum_ganjil, sumberair_minum_genap,
                                                          kec_airbersih_ganjil, kec_airbersih_genap, rombel_ganjil, rombel_genap, pd_ganjil, pd_genap, pd_lakiganjil, pd_lakigenap,
                                                          pd_perempuanganjil, pd_perempuangenap,pd_kelas1ganjil,pd_kelas1genap,pd_kelas2ganjil,pd_kelas2genap,pd_kelas3ganjil,pd_kelas3genap,
                                                          pd_kelas4ganjil,pd_kelas4genap,pd_kelas5ganjil,pd_kelas5genap,pd_kelas6ganjil,pd_kelas6genap,sinkron_terakhir, input_time)

                                            elif level == "smp":
                                                values = (tahun_ajaran, npsn, nama_skul, bentuk_pend, status_sekolah, skul_id, skul_id_enkrip, alamat, kecamatan, kabkot,
                                                          provinsi, ptk_lakiganjil, ptk_lakigenap, ptk_perempuanganjil, ptk_perempuangenap, ptk_ganjil, ptk_genap, pegawai_lakiganjil,
                                                          pegawai_lakigenap, pegawai_perempuanganjil, pegawai_perempuangenap, pegawai_ganjil, pegawai_genap, guru_kelasganjil, guru_kelasgenap,
                                                          guru_matematikaganjil, guru_matematikagenap, guru_bindoganjil, guru_bindogenap, guru_binggrisganjil, guru_binggrisgenap,
                                                          guru_sejarahganjil, guru_sejarahgenap, guru_pknganjil, guru_pkngenap, guru_penjaskesganjil, guru_penjaskesgenap, guru_agamaganjil,
                                                          guru_agamagenap, guru_seniganjil, guru_senigenap, before_RKelas_ganjil, before_RKelas_genap, after_Rkelas_ganjil, after_Rkelas_genap,
                                                          before_RPerpus_ganjil, before_RPerpus_genap, after_RPerpus_ganjil, after_RPerpus_genap, before_RLab_ganjil, before_RLab_genap,
                                                          after_RLab_ganjil, after_RLab_genap, before_RPraktik_ganjil, before_RPraktik_genap, after_RPraktik_ganjil, after_RPraktik_genap,
                                                          before_RGuru_ganjil, before_RGuru_genap, after_RGuru_ganjil, after_RGuru_genap, before_RIbadah_ganjil, before_RIbadah_genap,
                                                          after_RIbadah_ganjil, after_RIbadah_genap, before_RUKS_ganjil, before_RUKS_genap, after_RUKS_ganjil, after_RUKS_genap,
                                                          before_RSirkulasi_ganjil, before_RSirkulasi_genap, after_RSirkulasi_ganjil, after_RSirkulasi_genap, before_TMainOlahraga_ganjil,
                                                          before_TMainOlahraga_genap, after_TMainOlahraga_ganjil, after_TMainOlahraga_genap, before_Bangunan_ganjil, before_Bangunan_genap,
                                                          after_Bangunan_ganjil, after_Bangunan_genap, sumberair_ganjil, sumberair_genap, sumberair_minum_ganjil, sumberair_minum_genap,
                                                          kec_airbersih_ganjil, kec_airbersih_genap, rombel_ganjil, rombel_genap, pd_ganjil, pd_genap, pd_lakiganjil, pd_lakigenap,
                                                          pd_perempuanganjil, pd_perempuangenap,pd_kelas7ganjil,pd_kelas7genap,pd_kelas8ganjil,pd_kelas8genap,pd_kelas9ganjil,
                                                          pd_kelas9genap,sinkron_terakhir, input_time)

                                            elif level == "sma":
                                                values = (tahun_ajaran, npsn, nama_skul, bentuk_pend, status_sekolah, skul_id, skul_id_enkrip, alamat, kecamatan, kabkot,
                                                          provinsi, ptk_lakiganjil, ptk_lakigenap, ptk_perempuanganjil, ptk_perempuangenap, ptk_ganjil, ptk_genap, pegawai_lakiganjil,
                                                          pegawai_lakigenap, pegawai_perempuanganjil, pegawai_perempuangenap, pegawai_ganjil, pegawai_genap, guru_kelasganjil, guru_kelasgenap,
                                                          guru_matematikaganjil, guru_matematikagenap, guru_bindoganjil, guru_bindogenap, guru_binggrisganjil, guru_binggrisgenap,
                                                          guru_sejarahganjil, guru_sejarahgenap, guru_pknganjil, guru_pkngenap, guru_penjaskesganjil, guru_penjaskesgenap, guru_agamaganjil,
                                                          guru_agamagenap, guru_seniganjil, guru_senigenap, before_RKelas_ganjil, before_RKelas_genap, after_Rkelas_ganjil, after_Rkelas_genap,
                                                          before_RPerpus_ganjil, before_RPerpus_genap, after_RPerpus_ganjil, after_RPerpus_genap, before_RLab_ganjil, before_RLab_genap,
                                                          after_RLab_ganjil, after_RLab_genap, before_RPraktik_ganjil, before_RPraktik_genap, after_RPraktik_ganjil, after_RPraktik_genap,
                                                          before_RGuru_ganjil, before_RGuru_genap, after_RGuru_ganjil, after_RGuru_genap, before_RIbadah_ganjil, before_RIbadah_genap,
                                                          after_RIbadah_ganjil, after_RIbadah_genap, before_RUKS_ganjil, before_RUKS_genap, after_RUKS_ganjil, after_RUKS_genap,
                                                          before_RSirkulasi_ganjil, before_RSirkulasi_genap, after_RSirkulasi_ganjil, after_RSirkulasi_genap, before_TMainOlahraga_ganjil,
                                                          before_TMainOlahraga_genap, after_TMainOlahraga_ganjil, after_TMainOlahraga_genap, before_Bangunan_ganjil, before_Bangunan_genap,
                                                          after_Bangunan_ganjil, after_Bangunan_genap, sumberair_ganjil, sumberair_genap, sumberair_minum_ganjil, sumberair_minum_genap,
                                                          kec_airbersih_ganjil, kec_airbersih_genap, rombel_ganjil, rombel_genap, pd_ganjil, pd_genap, pd_lakiganjil, pd_lakigenap,
                                                          pd_perempuanganjil, pd_perempuangenap,pd_kelas10ganjil,pd_kelas10genap,pd_kelas11ganjil,pd_kelas11genap,pd_kelas12ganjil,
                                                          pd_kelas12genap,merge_wilayah,sinkron_terakhir, input_time)


                                            elif level == "smk":
                                                values = (tahun_ajaran, npsn, nama_skul, bentuk_pend, status_sekolah, skul_id, skul_id_enkrip, alamat, kecamatan, kabkot,
                                                          provinsi, ptk_lakiganjil, ptk_lakigenap, ptk_perempuanganjil, ptk_perempuangenap, ptk_ganjil, ptk_genap, pegawai_lakiganjil,
                                                          pegawai_lakigenap, pegawai_perempuanganjil, pegawai_perempuangenap, pegawai_ganjil, pegawai_genap, guru_kelasganjil, guru_kelasgenap,
                                                          guru_matematikaganjil, guru_matematikagenap, guru_bindoganjil, guru_bindogenap, guru_binggrisganjil, guru_binggrisgenap,
                                                          guru_sejarahganjil, guru_sejarahgenap, guru_pknganjil, guru_pkngenap, guru_penjaskesganjil, guru_penjaskesgenap, guru_agamaganjil,
                                                          guru_agamagenap, guru_seniganjil, guru_senigenap, before_RKelas_ganjil, before_RKelas_genap, after_Rkelas_ganjil, after_Rkelas_genap,
                                                          before_RPerpus_ganjil, before_RPerpus_genap, after_RPerpus_ganjil, after_RPerpus_genap, before_RLab_ganjil, before_RLab_genap,
                                                          after_RLab_ganjil, after_RLab_genap, before_RPraktik_ganjil, before_RPraktik_genap, after_RPraktik_ganjil, after_RPraktik_genap,
                                                          before_RGuru_ganjil, before_RGuru_genap, after_RGuru_ganjil, after_RGuru_genap, before_RIbadah_ganjil, before_RIbadah_genap,
                                                          after_RIbadah_ganjil, after_RIbadah_genap, before_RUKS_ganjil, before_RUKS_genap, after_RUKS_ganjil, after_RUKS_genap,
                                                          before_RSirkulasi_ganjil, before_RSirkulasi_genap, after_RSirkulasi_ganjil, after_RSirkulasi_genap, before_TMainOlahraga_ganjil,
                                                          before_TMainOlahraga_genap, after_TMainOlahraga_ganjil, after_TMainOlahraga_genap, before_Bangunan_ganjil, before_Bangunan_genap,
                                                          after_Bangunan_ganjil, after_Bangunan_genap, sumberair_ganjil, sumberair_genap, sumberair_minum_ganjil, sumberair_minum_genap,
                                                          kec_airbersih_ganjil, kec_airbersih_genap, rombel_ganjil, rombel_genap, pd_ganjil, pd_genap, pd_lakiganjil, pd_lakigenap,
                                                          pd_perempuanganjil, pd_perempuangenap,pd_kelas10ganjil,pd_kelas10genap,pd_kelas11ganjil,pd_kelas11genap,pd_kelas12ganjil,
                                                          pd_kelas12genap,pd_kelas13ganjil,pd_kelas13genap,sinkron_terakhir, input_time)


                                            else:
                                                values = (tahun_ajaran, npsn, nama_skul, bentuk_pend, status_sekolah, skul_id, skul_id_enkrip, alamat, kecamatan, kabkot,
                                                            provinsi, ptk_lakiganjil, ptk_lakigenap, ptk_perempuanganjil, ptk_perempuangenap, ptk_ganjil, ptk_genap, pegawai_lakiganjil,
                                                            pegawai_lakigenap, pegawai_perempuanganjil, pegawai_perempuangenap, pegawai_ganjil, pegawai_genap, guru_kelasganjil, guru_kelasgenap,
                                                            guru_matematikaganjil, guru_matematikagenap, guru_bindoganjil, guru_bindogenap, guru_binggrisganjil, guru_binggrisgenap,
                                                            guru_sejarahganjil, guru_sejarahgenap, guru_pknganjil, guru_pkngenap, guru_penjaskesganjil, guru_penjaskesgenap, guru_agamaganjil,
                                                            guru_agamagenap, guru_seniganjil, guru_senigenap, before_RKelas_ganjil, before_RKelas_genap, after_Rkelas_ganjil, after_Rkelas_genap,
                                                            before_RPerpus_ganjil, before_RPerpus_genap, after_RPerpus_ganjil, after_RPerpus_genap, before_RLab_ganjil, before_RLab_genap,
                                                            after_RLab_ganjil, after_RLab_genap, before_RPraktik_ganjil, before_RPraktik_genap, after_RPraktik_ganjil, after_RPraktik_genap,
                                                            before_RGuru_ganjil, before_RGuru_genap, after_RGuru_ganjil, after_RGuru_genap, before_RIbadah_ganjil, before_RIbadah_genap,
                                                            after_RIbadah_ganjil, after_RIbadah_genap, before_RUKS_ganjil, before_RUKS_genap, after_RUKS_ganjil, after_RUKS_genap,
                                                            before_RSirkulasi_ganjil, before_RSirkulasi_genap, after_RSirkulasi_ganjil, after_RSirkulasi_genap, before_TMainOlahraga_ganjil,
                                                            before_TMainOlahraga_genap, after_TMainOlahraga_ganjil, after_TMainOlahraga_genap, before_Bangunan_ganjil, before_Bangunan_genap,
                                                            after_Bangunan_ganjil, after_Bangunan_genap, sumberair_ganjil, sumberair_genap, sumberair_minum_ganjil, sumberair_minum_genap,
                                                            kec_airbersih_ganjil, kec_airbersih_genap, rombel_ganjil, rombel_genap, pd_ganjil, pd_genap, pd_lakiganjil, pd_lakigenap,
                                                            pd_perempuanganjil, pd_perempuangenap,sinkron_terakhir, input_time)


                                            checkDataDB(table_name,tahun_ajaran,npsn,insert_dataECY,update_dataECY, values)
                                            if level != "smk" :
                                                dapodik4data(table_name,skul_id_enkrip,tahun_ajaran)




                                    except requests.exceptions.JSONDecodeError as E:
                                        print("error decoding JSON : ", E)
                                        print(result_kec.content)
                                else:
                                    print("Request on KECAMATAN failed with status code:", result_kec.status_code)

                        except requests.exceptions.JSONDecodeError as E:
                            print("Request failed with status code:", result.status_code)
                    else:
                        print("Request on KABKOT failed  with status code:", result.status_code)

            except requests.exceptions.JSONDecodeError as E:
                print("error decoding JSON : ", E)
        else:
            print("Request on PROV failed with status code:", result_kota.status_code)

def menu():
    print("Scraping Dapodik Menu : \n1. KB\n2. TK\n3. SD\n4. SMP\n5. SMA\n6. SMK\n7. Exit")
    user = int(input("Please Select Level , Ex : 1 >> "))
    tahun_ajaran = input("Academic Year, Ex : 2022 >> ")
    progres_view = input("Progress View, Ex : 20221 For ODD AY >> ")
    print("Note!, Inputing end of range is +1 of the selected end, Ex: If u want to end in 30 input 31\nRange is based on the number of province in Dapodik, Default is 1 to 40 (39 [38 + Luar Negeri] + 1)")
    provStart, provEnd = map(int,input("Range of Pull seperated by space, Ex : 1 31 >> ").split())
    if user == 1:
        a = "dapodik_KB"
        aa = "kb"
        checkDB(a,user)
        scrapingECY(a,aa,tahun_ajaran,progres_view,provStart,provEnd)
        menu()
    elif user == 2:
        b = "dapodik_TK"
        bb = "tk"
        checkDB(b,user)
        scrapingECY(b,bb,tahun_ajaran,progres_view,provStart,provEnd)
        menu()
    elif user == 3:
        c = "dapodik_SD"
        cc = "sd"
        checkDB(c,user)
        scrapingECY(c,cc,tahun_ajaran,progres_view,provStart,provEnd)
        menu()
    elif user == 4:
        d = "dapodik_SMP"
        dd = "smp"
        checkDB(d,user)
        scrapingECY(d,dd,tahun_ajaran,progres_view,provStart,provEnd)
        menu()
    elif user == 5:
        e = "dapodik_SMA"
        ee = "sma"
        checkDB(e,user)
        scrapingECY(e,ee,tahun_ajaran,progres_view,provStart,provEnd)
        menu()
    elif user == 6:
        f = "dapodik_SMK"
        ff = "smk"
        checkDB(f,user)
        scrapingECY(f,ff,tahun_ajaran,progres_view,provStart,provEnd)
        menu()
    else:
        quit()

menu()