import email_msg as em
ret = em.test_send_receive( subject="Test Email", body="This is a test email from xStation5.", wait_seconds=30)

print(ret)