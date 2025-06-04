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
        self, *conditions: dict, indices: list[int] = None, **kwargs
    ) -> list[int]:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "filter"))

    def __len__(self):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "__len__"))

    def __next__(self):
        """Should raise StopIteration when end of table is reached"""
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "__next__"))

    def reset_cursor(self):
        """Return to first row"""
        raise NotImplementedError(
            not_impl_err_msg(self.__class__.__name__, "reset_cursor")
        )


class JobBackend(TableBackend):
    def add_file(self, job_id: int, file_id: int, channel: int = 0):
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "add_file"))

    def get_files(self, job_id: int | list[int]) -> list[int]:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "get_file"))


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
    def job(self) -> JobBackend:
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

    @property
    def tag(self) -> TableBackend:
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "tag"))

    @property
    def granularity(self) -> TableBackend:
        raise NotImplementedError(
            not_impl_err_msg(self.__class__.__name__, "granularity")
        )
