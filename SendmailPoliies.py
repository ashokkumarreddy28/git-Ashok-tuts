# This is a Databricks using PySpark for send email for policy renewal alert Document
from pyspark.sql.functions import date_add, current_date,col,to_date
import smtplib
df = spark.read.table('policies')
df1 = df.filter(to_date(col("policy_date")) <= date_add(current_date(), 6))
df2=df1.select('policy_name','email')
recipients = []
for record in df2.collect():
  recipients.append(record)

subject ="Policies renewal alert"
body = "Your policy has been expire soon , so please renewal"

def email_sender(subject, body, recipients):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "ashokkonkala28@gmail.com"
    sender_password = "fdcu vbgy qeiw jwki"

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        for recipient in recipients:
            message = f"Subject: {subject}\n\n{body}"
            server.sendmail(sender_email, recipient, message)
email_sender(subject, body, recipients)
