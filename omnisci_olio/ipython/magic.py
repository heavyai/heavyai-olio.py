from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic, needs_local_scope)
import omnisci_olio.pymapd


@magics_class
class OmniSciSqlMagic(Magics):

    @line_cell_magic
    @needs_local_scope
    def sql(self, line, cell=None, local_ns=None):
        con_var = None
        if cell:
            query = cell
            if line:
                params = line.split(' ')
                con_var = params[0]
        else:
            query = line
        if not con_var:
            con_var = 'con'
        con = local_ns[con_var]
        
        sql = cell if cell else line
        return omnisci_olio.pymapd.select(con, query.format(**local_ns))
