from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import mapped_column
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm.exc import DetachedInstanceError
from uuid import uuid4

class Base(DeclarativeBase):

    id = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=uuid4)

    def calc_repr(self, **fields):
        field_strings = []
        at_least_one_attached_attribute = False
        for key, field in fields.items():
            try:
                field_strings.append(f"{key}={field!r}")
            except DetachedInstanceError:
                field_strings.append(f"{key}=DetachedInstanceError")
            else:
                at_least_one_attached_attribute = True
        if at_least_one_attached_attribute:
            return f"<{self.__class__.__name__}({','.join(field_strings)})>"
        return f"<{self.__class__.__name__} {id(self)}>"