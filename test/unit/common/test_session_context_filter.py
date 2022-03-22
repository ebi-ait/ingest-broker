from unittest import TestCase
from threading import Thread
from broker.common.util.logging import SessionContextFilter



class TestSessionContextFilter(TestCase):
    def set_value(self, name, value):
        SessionContextFilter.set_submission_id(value)
        self.ctx[name] = SessionContextFilter.get_thread_local('submission_id')
    
    def no_value(self, name):     
        self.ctx[name] = SessionContextFilter.get_thread_local('submission_id')
    
    def test_in_current_thread(self):
        value = 15
        SessionContextFilter.set_submission_id(value)
        self.assertEqual(SessionContextFilter.get_thread_local('submission_id'), value)
    
    def test_set_value(self):
        self.ctx = {}
        value = 15
        th1 = Thread(target=self.set_value, args=('th1', value,))
        th1.start()
        th1.join()
        self.assertEqual(self.ctx['th1'], value)

    def test_no_value(self):
        self.ctx = {}
        th1 = Thread(target=self.no_value, args=('th1',))
        th1.start()
        th1.join()
        self.assertEqual(self.ctx['th1'], 'N/A')
    
    def test_child_different_than_current_thread(self):
        SessionContextFilter.set_submission_id('current_thread')
        self.ctx = {}
        th1 = Thread(target=self.set_value, args=('th1','child_thread'))
        th1.start()
        th1.join()
        self.assertEqual(self.ctx['th1'], 'child_thread')
        self.assertEqual(SessionContextFilter.get_thread_local('submission_id'), 'current_thread')
