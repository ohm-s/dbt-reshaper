from dbt.events.types import InfoLevel, ErrorLevel
from dbt.events.functions import fire_event, Event
import dbt

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


original_fire_event = dbt.events.functions.fire_event
dbt_event_ignore_codes = ['I031', 'I012', 'I011']
def fire_event_override(e: Event):
    global dbt_event_ignore_codes
    if e.code not in dbt_event_ignore_codes:
        original_fire_event(e)

dbt.events.functions.fire_event = fire_event_override