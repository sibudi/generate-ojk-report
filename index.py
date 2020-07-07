from datetime import date, datetime, timedelta

import base64
import csv
import errno
import helper
import json
import logging
import os
import oss2
import pyminizip


yesterday = date.today() if (datetime.now()+timedelta(hours=7)).strftime('%Y%m%d') == (date.today()+timedelta(days=1)).strftime('%Y%m%d') else date.today() - timedelta(days=1)

logger = {}
OSS_CONFIG = {}
def init(context):
    global logger
    global OSS_CONFIG
    logger = logging.getLogger()
    logger.info('Initializing. . .')

    # Get oss configuration
    OSS_CONFIG = helper.get_configuration(context, 'oss')
    creds = context.credentials
    auth = oss2.StsAuth(creds.accessKeyId, creds.accessKeySecret, creds.securityToken)
    helper.INTERNAL_BUCKET = oss2.Bucket(auth, OSS_CONFIG['internal_endpoint'], OSS_CONFIG['bucket_name'])
  
    # Get SQL connection
    config = helper.get_configuration(context, 'apsara')
    helper.SQL_CONNECTION = helper.connect_to_mysql(context, config)

    # Get send email config
    helper.NOTIFICATION_CONFIG = helper.get_configuration(context, 'generate-ojk-report')


def handler(event, context):
    try:
        environ = os.environ
        payload = json.loads(event)['payload']
        connection = helper.SQL_CONNECTION

        if payload == "SEND_TO_OJK":
        
            #delete
            with connection.cursor() as cursor:
                sql = """ delete from reporting.sik_fdc where tgl_pelaporan_data = subdate(current_date, 1) """

                cursor.execute(sql)

            connection.commit()
            logger.info("deleted")

            #insert
            with connection.cursor() as cursor:
                sql = """
                            insert into reporting.sik_fdc
                            select null id
                                , 820053 id_penyelenggara
                                , b.uuid id_borrower
                                , 1 jenis_pengguna
                                , b.realName nama_borrower
                                , substring(b.idCardNo, 1, 16) no_identitas
                                , null no_npwp
                                , a.uuid id_pinjaman
                                , date(COALESCE(c.statusChangeTime, a.lendingTime)) tgl_perjanjian_borrower
                                , date(a.lendingTime) tgl_penyaluran_dana
                                , a.amountApply nilai_pendanaan
                                , subdate(current_date, 1) tgl_pelaporan_data
                                , CASE
                                    WHEN (a.status in (10,11) AND a.orderType in (0,3))
                                        THEN 0
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (7,8))
                                        THEN f.amountApply
                                    WHEN (a.status in (7,8) AND a.orderType = 0)
                                        THEN a.amountApply
                                    WHEN (a.status in (7,8) AND a.orderType = 3)
                                        #THEN (PERIOD_DIFF(DATE_FORMAT(d.refundTime, '%Y%m'), DATE_FORMAT(a.refundTime, '%Y%m')) + 1) * d.billAmout
                                        THEN IF((a.refundTime = d.refundTime), d.billAmout, IF((a.refundTime = d1.refundTime), d1.billAmout * 3, d2.billAmout *2)) #edit20200210
                                    ELSE 0 #jika order ext sudah lunas
                                END sisa_pinjaman_berjalan
                                , IF (a.orderType <> 3, date(a.refundTime),
                                    IF(
                                        (timestamp(current_date) > d.refundTime), date(d.refundTime),
                                        IF (
                                            (timestamp(current_date) > d2.refundTime), date(d2.refundTime),
                                                date(d1.refundTime)
                                        )
                                    )
                                ) tgl_jatuh_tempo_pinjaman #update_20200615
                                , CASE 
                                    WHEN (a.status in (10,11) AND datediff(a.actualRefundTime, date(a.refundTime)) < 30 AND a.orderType in (0,3)) #update_20200615
                                        OR (a.status in (7,8) AND a.orderType = 0 AND datediff(subdate(current_date, 1), date(a.refundTime)) < 30) #tambahan update_20200615
                                        OR (a.status in (7,8) AND a.orderType = 3 
                                            AND IF(
                                                    (timestamp(current_date) > d.refundTime), datediff(subdate(current_date, 1), date(d.refundTime)) < 30,
                                                        IF (
                                                            (timestamp(current_date) > d2.refundTime), datediff(subdate(current_date, 1), date(d2.refundTime)) < 30,
                                                                datediff(subdate(current_date, 1), date(d1.refundTime)) < 30
                                                        )
                                                )											
                                            ) #tambahan update_20200615
                                        OR (a.status in (10,11) AND f.status in (7,8,10,11) AND datediff(COALESCE(f.actualRefundTime, subdate(current_date, 1)), date(f.refundTime)) < 30 AND a.orderType = 2)
                                        THEN 1
                                    WHEN (a.status in (10,11) AND (COALESCE(datediff(a.actualRefundTime, a.refundTime), 0) >= 30 AND COALESCE(datediff(a.actualRefundTime, a.refundTime), 0) <= 90) AND a.orderType in (0,3)) 
                                        OR (a.status in (7,8) AND (datediff(subdate(current_date, 1), date(a.refundTime)) >= 30 AND datediff(subdate(current_date, 1), date(a.refundTime)) <= 90) AND a.orderType = 0) #update_20200615
                                        OR (a.status in (7,8) AND IF(
                                                                    (timestamp(current_date) > d.refundTime), datediff(subdate(current_date, 1), date(d.refundTime)) >= 30 AND datediff(subdate(current_date, 1), date(d.refundTime)) <= 90,
                                                                        IF(
                                                                            (timestamp(current_date) > d2.refundTime), datediff(subdate(current_date, 1), date(d2.refundTime)) >= 30 AND datediff(subdate(current_date, 1), date(d2.refundTime)) <= 90,
                                                                                datediff(subdate(current_date, 1), date(d1.refundTime)) >= 30 AND datediff(subdate(current_date, 1), date(d1.refundTime)) <= 90
                                                                        )
                                                                ) AND a.orderType = 3) #tambahan update_20200615
                                        OR (a.status in (10,11) AND f.status in (10,11) AND (datediff(f.actualRefundTime, f.refundTime) >= 30 AND datediff(f.actualRefundTime, f.refundTime) <= 90) AND a.orderType = 2)
                                        OR (a.status in (10,11) AND f.status in (7,8) AND (datediff(subdate(current_date, 1), date(f.refundTime)) >= 30 AND datediff(subdate(current_date, 1), date(f.refundTime)) <= 90) AND a.orderType = 2)
                                        THEN 2
                                    ELSE 3
                                END id_kualitas_pinjaman
                                , CASE
                                    WHEN (a.status in (7,8) AND a.orderType = 0) #update_20200615
                                        THEN greatest(datediff(subdate(current_date, 1), date(a.refundTime)), 0)
                                    WHEN (a.status in (7,8) AND a.orderType = 3) #tambahan update_20200615
                                        THEN IF(
                                                (timestamp(current_date) > d.refundTime), greatest(datediff(subdate(current_date, 1), date(d.refundTime)), 0), 
                                                    IF(
                                                        (timestamp(current_date) > d2.refundTime), greatest(datediff(subdate(current_date, 1), date(d2.refundTime)), 0), 
                                                            greatest(datediff(subdate(current_date, 1), date(d1.refundTime)), 0)
                                                    )
                                            )
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (7,8)) 
                                        THEN greatest(COALESCE(datediff(subdate(current_date, 1), date(f.refundTime)), 0), 0) #jika order ext blum lunas, hari keterlambatan terakhir = hari keterlambatan orderType 1
                                    WHEN (a.status in (10,11) AND a.orderType in (0,3)) 
                                        THEN greatest(coalesce(datediff(a.actualRefundTime, a.refundTime), 0), 0) #jika order normal/cicilan lunas, hari keterlambatan terakhir = actualRefundTime - refundTime
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (10,11)) 
                                        THEN greatest(coalesce(datediff(f.actualRefundTime, f.refundTime), 0), 0) #jika order ext lunas, hari keterlambatan terakhir = actualRefundTime - refundTime yg kedua
                                    ELSE 0
                                END status_pinjaman_dpd
                                , CASE
                                    WHEN (a.status in (7,8) AND a.orderType = 0)
                                        THEN greatest(COALESCE(datediff(subdate(current_date, 1), date(a.refundTime)), 0), 0)
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (7,8)) 
                                        THEN IF(COALESCE(datediff(subdate(current_date, 1), date(f.refundTime)), 0) < datediff(a.actualRefundTime, a.refundTime), 
                                            greatest(COALESCE(datediff(a.actualRefundTime, a.refundTime), 0), 0), 
                                            greatest(COALESCE(datediff(subdate(current_date, 1), date(f.refundTime)), 0), 0)
                                        )
                                    WHEN (a.status in (10,11) AND a.orderType = 0) 
                                        THEN greatest(coalesce(datediff(a.actualRefundTime, a.refundTime), 0), 0)
                                    WHEN (a.status in (10,11) AND a.orderType = 2 AND f.status in (10,11)) 
                                        THEN IF(greatest(coalesce(datediff(f.actualRefundTime, f.refundTime), 0), 0) < datediff(a.actualRefundTime, a.refundTime), 
                                            greatest(COALESCE(datediff(a.actualRefundTime, a.refundTime), 0), 0), 
                                            greatest(COALESCE(datediff(f.actualRefundTime, f.refundTime), 0), 0)
                                        )
                                    WHEN (a.status in (7,8) AND a.orderType = 3)
                                        THEN IF(d1.actualRefundTime is NULL, 
                                            greatest(COALESCE(datediff(subdate(current_date, 1), date(d1.refundTime)), 0), 0), 
                                            IF(d2.actualRefundTime is NULL, 
                                                greatest(coalesce(datediff(subdate(current_date, 1), date(d2.refundTime)), 0), coalesce(datediff(date(d1.actualRefundTime), date(d1.refundTime)), 0), 0), #edit20200210
                                                greatest(coalesce(datediff(subdate(current_date, 1), date(d.refundTime)), 0), coalesce(datediff(date(d1.actualRefundTime), date(d1.refundTime)), 0), coalesce(datediff(date(d2.actualRefundTime), date(d2.refundTime)), 0), 0) #edit20200210
                                            )
                                        )
                                    WHEN (a.status in (10,11) AND a.orderType = 3)
                                        THEN greatest(coalesce(datediff(a.actualRefundTime, d.refundTime), 0), coalesce(datediff(d2.actualRefundTime, d2.refundTime), 0), coalesce(datediff(d1.actualRefundTime, d1.refundTime), 0), 0)
                                    ELSE 0
                                END status_pinjaman_max_dpd
                                , CASE 
                                    WHEN (a.status in (10,11) AND a.orderType in (0,3)) 
                                        OR (a.status in (10,11) AND a.orderType = 2 AND f.status in (10,11))
                                            THEN 'L' #kalo nasabah bayar di hari yg sama query dijalankan, dia tidak akan terambil, krn pake filter date(a.updateTime) < CURRENT_DATE
                                            #tapi harusnya dia tetap terambil dengan status O (kalo status W aman krn tdk diambil ulang)
                                    WHEN (a.status in (7,8) AND a.orderType = 0 AND datediff(subdate(current_date, 1), date(a.refundTime)) <= 90) 
                                        OR (a.status in (7,8) AND a.orderType = 3 AND datediff(subdate(current_date, 1), date(d.refundTime)) <= 90)
                                        OR (a.status in (10,11) AND a.orderType = 2 AND datediff(subdate(current_date, 1), date(f.refundTime)) <= 90 AND f.status in (7,8))
                                            THEN 'O'
                                    ELSE 'W'
                                END status_pinjaman
                                , date(a.updateTime)
                            from ordOrder a
                            left join usrUser b
                                on a.userUuid = b.uuid
                            left join ordHistory c
                                on a.uuid = c.orderId
                                and c.status = 20
                                and c.updateTime = (select max(c2.updateTime) from ordHistory c2 where c2.orderId = c.orderId and c2.status = 20 and c2.disabled = 0)
                            left join ordBill d
                                on a.uuid = d.orderNo
                                and a.orderType = 3
                                and d.billTerm = 3
                            left join ordBill d1
                                on a.uuid = d1.orderNo
                                and a.orderType = 3
                                and d1.billTerm = 1
                            left join ordBill d2
                                on a.uuid = d2.orderNo
                                and a.orderType = 3
                                and d2.billTerm = 2
                            left join ordDelayRecord e
                                on a.uuid = e.orderNo
                            left join ordOrder f
                                on e.delayOrderNo = f.uuid
                            where 1
                            and a.disabled = 0
                            and b.disabled = 0
                            and a.status in (7, 8, 10, 11)
                            and a.orderType in (0, 2, 3)
                            and (
                                (date(a.updateTime) = subdate(CURRENT_DATE,1) and a.orderType in (0, 1))	#update_20200615
                                or (datediff(subdate(current_date, 1), date(a.refundTime)) <= 91 and a.status in (7,8) and a.orderType = 0)	#update_20200615
                                or (datediff(subdate(current_date, 1), date(f.refundTime)) <= 91 and f.status in (7,8))
                                or (a.orderType = 3 and a.status in (10,11) and date(a.actualRefundTime) = subdate(current_date, 1))
                                or (a.orderType = 3 and a.status in (7, 8) and datediff(subdate(current_date, 1), date(d.refundTime)) <= 91)# tambahan update_20200615
                            )
                """
                cursor.execute(sql)

            connection.commit()
            logger.info("inserted")

            #read
            with connection.cursor() as cursor:
                sql = """
                        SELECT id_penyelenggara,
                            id_borrower,
                            jenis_pengguna,
                            nama_borrower,
                            no_identitas,
                            no_npwp,
                            id_pinjaman,
                            date_format(tgl_perjanjian_borrower, '%Y%m%d') tgl_perjanjian_borrower,
                            date_format(tgl_penyaluran_dana, '%Y%m%d') tgl_penyaluran_dana,
                            nilai_pendanaan,
                            date_format(tgl_pelaporan_data, '%Y%m%d') tgl_pelaporan_data,
                            sisa_pinjaman_berjalan,
                            date_format(tgl_jatuh_tempo_pinjaman, '%Y%m%d') tgl_jatuh_tempo_pinjaman,
                            id_kualitas_pinjaman,
                            status_pinjaman_dpd,
                            status_pinjaman_max_dpd,
                            status_pinjaman
                        FROM reporting.sik_fdc 
                        where tgl_pelaporan_data = subdate(current_date,1)
                """
                cursor.execute(sql)
                data_csv = cursor.fetchall()
                column_name = cursor.description
                column_name = [tupl[0] for tupl in list(column_name)]
                logger.info("read")

            #read counter
            sikCounter = read_counter(connection, 0)

            filename = helper.NOTIFICATION_CONFIG['zip_username'] + yesterday.strftime('%Y%m%d') + 'SIK' + sikCounter
            filepath = environ['HOME'] + '/' +  filename
            logger.info(f"{filename} {filepath}")

            with open(filepath + '.csv', mode='w', newline='') as f:
                writer = csv.DictWriter(f, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL, fieldnames=column_name)
                for row in data_csv:
                    writer.writerow(row)
        
            #zipfile
            zip_password = helper.decrypt_string(context, helper.NOTIFICATION_CONFIG['zip_password'])
            pyminizip.compress(filepath + '.csv', None, filepath + '.zip' , zip_password, 0)
            # last arg: compress_level(int) between 1 to 9, 1 (more fast) <---> 9 (more compress) or 0 (default)

            #upload to oss
            logger.info('Upload zip file to OSS')
            helper.simple_upload(filepath + '.zip', "send-to-ojk/upload/" + filename + '.zip')

            #delete file csv
            silentremove(filepath + '.csv')
            #delete file zip
            silentremove(filepath + '.zip')


        elif payload == "CHECK_OJK_RESULT":
            sikCounter = read_counter(connection, 1)
            
            filename = []
            content = []
            printf = []

            filename.append(helper.NOTIFICATION_CONFIG['zip_username'] + yesterday.strftime('%Y%m%d') + 'SIK' + sikCounter + '.zip.out')
            filename.append('statistic_' + helper.NOTIFICATION_CONFIG['zip_username'] + '.json')

            for item in filename:
                oss_file_name = "send-to-ojk/download/" + item
                fileStream = helper.downloadFileStreamFromOss(oss_file_name)

                printf.append(fileStream)


            content = map(base64.b64encode, printf)
            printf = [item.decode('utf-8') for item in printf]

            #read db for construct message
            with connection.cursor() as cursor:
                sql = """
                       select tgl_pelaporan_data, count(*) count
                       from reporting.sik_fdc 
                       where tgl_pelaporan_data >= subdate(current_date, 7) and tgl_pelaporan_data <= subdate(current_date,1) 
                       group by tgl_pelaporan_data 
                       order by 1
                """
                cursor.execute(sql)
                data = cursor.fetchall()

            message = f"Terlampir hasil output dari folder out sftp pusdafil.<br /><br />"
            message += f"<table style='width: 30%;' border='2' cellpadding='1'>"
            message += f"<tr><th>tanggal</th> <th>count</th></tr>"
            for row in data: #print as row
                message += f"<tr><td>{row['tgl_pelaporan_data']}</td> <td>{row['count']}</td></tr>"
            
            message += f"</table>"
            message += f"<p>{filename[0]}:<br /> <pre><div style='padding-left:2em'>{printf[0]}</div></pre></p>"
            try:
                printf_json = json.loads(printf[1])
            except ValueError as e:
                printf_json = [{}]
                logger.error('invalid json: %s' % e)
                
            message += f"<p>{filename[1]}:<br />" 
            message += f"<pre><div style='padding-left:2em'>"
            for key in printf_json[0]:
                message += f"{key}: {printf_json[0][key]}<br/>"
            
            message += f"</div></pre></p>"

            #send notification
            content = [item.decode('utf-8') for item in content]
            attachments = [*zip(filename, content)]
            send_notification(attachments, message, environ, context)


    except Exception as e:
        import traceback
        logger.error(e)
        logger.error(traceback.format_exc())


def silentremove(filename):
    try:
        os.remove(filename)
        logger.info("sukses delete " + filename)
    except OSError as e:
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occurred


def read_counter(connection, check_only):
    with connection.cursor() as cursor:
        sql = "select counter from reporting.sik_fdc_dailycounter where tgl_pelaporan_data = subdate(current_date, 1)"
        cursor.execute(sql)
        data = cursor.fetchone()

    if (check_only == 1):
        return str(data['counter']).zfill(2) if (data is not None) else '00'

    counter = 0
    sikCounter = ''
    if (data is not None):
        logger.info(f"last counter = {data['counter']}")
        counter = data['counter'] + 1
        sikCounter = str(counter).zfill(2)

        with connection.cursor() as cursor:
            sql = f"update reporting.sik_fdc_dailycounter set counter = {counter} where tgl_pelaporan_data = subdate(current_date, 1)"
            cursor.execute(sql)

    else:
        logger.info(f"last counter = 0")
        counter = 1
        sikCounter = '01'

        with connection.cursor() as cursor:
            sql = f"insert into reporting.sik_fdc_dailycounter (tgl_pelaporan_data, counter) values (subdate(current_date,1), 1)"
            cursor.execute(sql)
    
    connection.commit()
    logger.info(f"current counter = {counter} ; sikfile = {sikCounter}")
    logger.info("updated or inserted")
    
    return sikCounter


def send_notification(attachments, message, environ, context):
    subject = f"SIK FDC Notification {yesterday.strftime('%Y%m%d')}"
    to = helper.NOTIFICATION_CONFIG['to']
    cc = helper.NOTIFICATION_CONFIG['cc'] if 'cc' in helper.NOTIFICATION_CONFIG else ""
    
    # convert list of tuples into list of dictionary
    keys = ('filename', 'content')
    attachments = [dict(zip(keys, values)) for values in attachments]
    
    helper.send_email(context.function.name, subject, message, to, cc, None, attachments)