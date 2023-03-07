from django.core.mail import EmailMessage
from backend_estaciones.settings import EMAIL_HOST_USER, AUTHENTICATION_360_NRS
import re, requests, json
from twilio.rest import Client


def send_email(data):
        email = EmailMessage(subject= data['email_subject'], body=data['template'], to=[data['to_email']], from_email=EMAIL_HOST_USER)
        
        email.content_subtype ='html'
        response = email.send(fail_silently=True)
        print(email)
        print(response)
        return response

def send_sms(phone, sms):
        url = "https://dashboard.360nrs.com/api/rest/sms"

        telefono = phone
        mensaje = sms
        telefono = telefono.replace("+","")
        print(telefono)
        print(len(sms))
    

        payload = {"to": [telefono], "from": "TEST", "message": mensaje}
        payload = json.dumps(payload).encode('utf-8')

        headers = {
        'Content-Type': 'application/json', 
        'Authorization': 'Basic ' + AUTHENTICATION_360_NRS
        }
        
        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)

#Envio de whatsapp
# def send_whatsapp_message(phone_numbers, message):
#     account_sid = 'ACfb7529765a03940585735c7821da83ff'
#     auth_token = '5ffa40644ec5cbf4e1ff457d2190642d'
#     client = Client(account_sid, auth_token)
    
#     for phone_number in phone_numbers:
#         message = client.messages.create(
#           from_='whatsapp:+14155238886',
#           body=message,
#           to=f'whatsapp:+{phone_number}'
#         )
#         print(f'Message sent to {phone_number}. SID: {message.sid}')
 