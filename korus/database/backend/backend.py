from korus.util import not_impl_err_msg


class TableBackend:

    def __init__(self, name: str):
        self.name = name

    def get(
        self,
        indices: int | list[int] = None,
        fields: str | list[str] = None,
        return_indices: bool = False,
    ) -> list[tuple]:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "get"))

    def add(self, row: dict):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "add"))

    def set(self, idx: int, row: dict):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "set"))

    def filter(
        self,
        condition: dict = None,
        invert: bool = False,
        indices: list[int] = None,
        **kwargs
    ) -> list[int]:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "filter"))

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

    @property
    def label(self) -> TableBackend:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "label"))
