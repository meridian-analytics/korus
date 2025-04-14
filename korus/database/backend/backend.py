from korus.util import not_impl_err_msg


class TableBackend():
    def get(self):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "get"))

    def add(self):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "add"))

    def set(self):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "set"))

    def filter(self):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "filter"))


class DatabaseBackend:

    @property
    def deployment(self) -> TableBackend:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "deployment"))

    @property
    def annotation(self) -> TableBackend:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "annotation"))

    @property
    def file(self) -> TableBackend:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "file"))

    @property
    def job(self) -> TableBackend:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "job"))

    @property
    def storage(self) -> TableBackend:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "storage"))

    @property
    def taxonomy(self) -> TableBackend:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "taxonomy"))
