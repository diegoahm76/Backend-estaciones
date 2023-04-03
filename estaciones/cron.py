import datetime

def test_cronjob():
    with open("/tmp/cronlog.txt", "a") as f:
        f.write("test_cronjob() called at {}\n".format(datetime.datetime.now()))
    
    msg = "FUNCIONA?"
    print(msg)
    return msg
