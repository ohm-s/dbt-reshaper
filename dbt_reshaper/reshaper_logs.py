from dbt.events.types import InfoLevel, ErrorLevel
from dbt.events.functions import fire_event

class ReshaperInfoLevel(InfoLevel):
    def code(self):
        return "DR-XX"
    def message(self) -> str:
        return "ReshaperInfoLevel"

class ReshaperErrorLevel(ErrorLevel):
    def code(self):
        return "DR-XX"
    def message(self) -> str:
        return "ReshaperErrorLevel"

def fire_info_event(code, message):
    event = ReshaperInfoLevel()
    event.code = lambda: code
    event.message = lambda: message
    fire_event(event)

def fire_error_event(code, message):
    event = ReshaperErrorLevel()
    event.code = lambda: code
    event.message = lambda: message
    fire_event(event)