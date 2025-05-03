from korus.util import not_impl_err_msg


class TableBackend:
    def get(self, indices: int | list[int] = None, fields: str | list[str] = None):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "get"))

    def add(self, row: dict):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "add"))

    def set(self, idx: int, row: dict):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "set"))

    def filter(self, *args, **kwargs):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "filter"))

    def add_field(
        self,
        name: str,
        type: "typing.Any",
        description: str,
        default: "typing.Any" = None,
        required: bool = True,
    ):
        raise NotImplementedError(
            not_impl_err_msg(self.__class__.__name__, "add_field")
        )

    def __len__(self):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "__len__"))


class DatabaseBackend:

    @property
    def deployment(self) -> TableBackend:
        raise NotImplementedError(
            not_impl_err_msg(self.__class__.__name__, "deployment")
        )

    @property
    def annotation(self) -> TableBackend:
        raise NotImplementedError(
            not_impl_err_msg(self.__class__.__name__, "annotation")
        )

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
