from omnisci_olio.schema import ModelObject


class Dashboard (ModelObject):
    """
    Note, this class does not directly manage or deal with the Immerse dashboard objects,
    it is strictly for modeling relationships.
    """

    def __init__(self, name, tables=None, tags=None):
        self.tables = tables
        super().__init__(name=name, tags=tags)
