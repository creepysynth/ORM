import psycopg2
import psycopg2.extras

class DatabaseError(Exception):
    pass
class NotFoundError(Exception):
    pass

class Entity(object):
    db = None

    # ORM part 1
    __delete_query    = 'DELETE FROM "{table}" WHERE {table}_id=%s'
    __insert_query    = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) RETURNING "{table}_id"'
    __list_query      = 'SELECT * FROM "{table}"'
    __select_query    = 'SELECT * FROM "{table}" WHERE {table}_id=%s'
    __update_query    = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s'

    # ORM part 2
    __parent_query    = 'SELECT * FROM "{table}" WHERE {parent}_id=%s'
    __sibling_query   = 'SELECT * FROM "{sibling}" NATURAL JOIN "{join_table}" WHERE {table}_id=%s'
    __update_children = 'UPDATE "{table}" SET {parent}_id=%s WHERE {table}_id IN ({children})'

    def __init__(self, id=None):
        if self.__class__.db is None:
            raise DatabaseError()

        self.__cursor   = self.__class__.db.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )

        self.__fields   = {}
        self.__id       = id
        self.__loaded   = False
        self.__modified = False
        self.__table    = self.__class__.__name__.lower()
    
    def __getattr__(self, name):
        # check, if instance is modified and throw an exception
        # get corresponding data from database if needed
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    getter with name as an argument
        # throw an exception, if attribute is unrecognized
        if self.__modified:
            raise DatabaseError()

        self.__load()
        if name in self._columns:
            return self._get_column(name)
        if name in self._children:
            return self._get_children(name)
        if name in self._parents:
            return self._get_parent(name)
        if name in self._siblings:
            return self._get_siblings(name)
        raise NotFoundError()

    def __setattr__(self, name, value):
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    setter with name and value as arguments or use default implementation
        if name in self._columns:
            self._set_column(name, value)
        elif name in self._parents:
            self._set_parent(name, value)
        else:
            super(Entity, self).__setattr__(name, value)
    
    def __execute_query(self, query, args):
        # execute an sql statement and handle exceptions together with transactions
        try:
            self.__cursor.execute(query, args)
        except:
            self.__class__.db.rollback()
            raise DatabaseError()

    def __insert(self):
        # generate an insert query string from fields keys and values and execute it
        # use prepared statements
        # save an insert id
        if self.__fields:
            cols = ', '.join(self.__fields.keys())
            plhs = ', '.join(
                '\'{}\''.format(value) for value in self.__fields.values()
            )
            
            self.__execute_query(
                self.__insert_query.format(
                    table=self.__table,columns=cols, placeholders=plhs
                ),
                None
            )        
            self.__id = self.__cursor.fetchone()[0]
        
    def __load(self):
        # if current instance is not loaded yet - execute select statement and store
        # it's result as an associative array (fields), where column names used as keys
        if not self.__id:
            raise NotFoundError()
        if not self.__loaded:
            self.__execute_query(
                self.__select_query.format(table=self.__table),
                (self.__id,)
            )
            self.__fields = dict(self.__cursor.fetchone())
            self.__loaded = True

    def __update(self):
        # generate an update query string from fields keys and values and execute it
        # use prepared statements
        cols = ', '.join(
            '{} = \'{}\''.format(key, self.__fields[key]) for key in self.__fields
        )

        self.__execute_query(
            self.__update_query.format(table=self.__table, columns=cols),
            (self.__id,)
        )

    def _get_children(self, name):
        # return an array of child entity instances
        # each child instance must have an id and be filled with data
        from models import Section, Category, Post, Comment, Tag, User
        
        child = self._children[name].lower()
        instances = []

        self.__execute_query(
            self.__parent_query.format(table=child, parent=self.__table),
            (self.__id,)
        )
        rows = self.__cursor.fetchall()
        
        for i in range(0, len(rows)):
            id = rows[i]['{}_id'.format(child)]
            instance = eval(child.title())(id)
            instance.__fields = rows[i]
            instance.__loaded = True
            instances.append(instance)
        return instances

    def _get_column(self, name):
        # return value from fields array by <table>_<name> as a key  
        return self.__fields['{}_{}'.format(self.__table, name)]

    def _get_parent(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an instance of parent entity class with an appropriate id
        from models import Section, Category, Post, Comment, Tag, User
        
        parent_id = self.__fields['{}_id'.format(name)]

        self.__execute_query(
            self.__select_query.format(table=name),
            (parent_id,)
        )
        row = dict(self.__cursor.fetchone())

        instance = eval(name.title())(parent_id)
        instance.__fields = row
        instance.__loaded = True

        return instance
        
    def _get_siblings(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an array of sibling entity instances
        # each sibling instance must have an id and be filled with data
        from models import Section, Category, Post, Comment, Tag, User

        sibling = self._siblings[name].lower()
        join_table = '{}__{}'.format(*sorted([self.__table, sibling]))
        instances = []
        
        self.__execute_query(
            self.__sibling_query.format(
                sibling=sibling,
                join_table=join_table,
                table=self.__table
            ), 
            (self.__id,)
        )
        rows = self.__cursor.fetchall()

        for i in range(0, len(rows)):
            id = rows[i]['{}_id'.format(sibling)]
            instance = eval(sibling.title())(id)
            instance.__fields = rows[i]
            instance.__loaded = True
            instances.append(instance)
        
        return instances

    def _set_column(self, name, value):
        # put new value into fields array with <table>_<name> as a key
        if self.__id:
            self.__load()
        self.__fields['{}_{}'.format(self.__table, name)] = value
        self.__modified = True

    def _set_parent(self, name, value):
        # ORM part 2
        # put new value into fields array with <name>_id as a key
        # value can be a number or an instance of Entity subclass
        if self.__id:
            self.__load()
        if type(value) is int:
            self.__fields['{}_id'.format(name)] = value
        else:
            self.__fields['{}_id'.format(name)] = value.__id
        self.__modified = True
    
    @classmethod
    def all(cls):
        # get ALL rows with ALL columns from corrensponding table
        # for each row create an instance of appropriate class
        # each instance must be filled with column data, a correct id and
        # MUST NOT query a database for own fields any more
        # return an array of istances
        cls_obj = cls()
        instances = []

        cls_obj.__execute_query(
            cls_obj.__list_query.format(table=cls_obj.__table),
            None
        )
        rows = cls_obj.__cursor.fetchall()

        for i in range(0, len(rows)):
            id = rows[i]['{}_id'.format(cls.__name__.lower())]
            instance = cls(id)        
            instance.__fields = rows[i]
            instance.__loaded = True
            instances.append(instance)
        
        return instances

    def delete(self):
        # execute delete query with appropriate id
        if self.__id:
            self.__execute_query(
                self.__delete_query.format(table=self.__table),
                (self.__id,)
            )
            self.__modified = True
            self.__id = None
        else:
            raise NotFoundError()

    @property
    def id(self):
        # try to guess yourself
        return self.__id

    @property
    def created(self):
        # try to guess yourself
        if self.__id:
            return self._get_column('created')

    @property
    def updated(self):
        # try to guess yourself
        if self.__id:
            return self._get_column('updated')

    def save(self):
        # execute either insert or update query, depending on instance id
        if self.__modified:
            if self.__id:
                self.__update()
            else:
                self.__insert()
            self.__class__.db.commit()
            self.__modified = False
