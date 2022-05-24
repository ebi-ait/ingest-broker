import logging
import threading


thread_local = threading.local()

SUBMISSION_ID_KEY = 'submission_id'
    
class SessionContextFilter(logging.Filter):

    def filter(self, record):
        record.submission_id = SessionContextFilter.get_thread_local(SUBMISSION_ID_KEY)
        return True

    @staticmethod
    def get_thread_local(key):
        return getattr(thread_local, key, 'N/A')
    
    @staticmethod
    def set_thread_local(key, value):
        return setattr(thread_local, key, value)

    @staticmethod
    def set_submission_id(submission_id):
        SessionContextFilter.set_thread_local(SUBMISSION_ID_KEY, submission_id)