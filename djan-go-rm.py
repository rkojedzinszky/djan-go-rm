import os
import argparse
import pathlib
from typing import List, Mapping

import jinja2
from django.apps import AppConfig, apps
from django.db import models
from django.db.models import fields
from django.db.models.options import Options

GO_BOOL = "bool"
GO_NULLBOOL = "sql.NullBool"
GO_INT64 = "int64"
GO_NULLINT64 = "sql.NullInt64"
GO_INT32 = "int32"
GO_NULLINT32 = "sql.NullInt32"
GO_FLOAT64 = "float64"
GO_NULLFLOAT64 = "sql.NullFloat64"
GO_DATETIME = "time.Time"
GO_NULLDATETIME = "sql.NullTime"
GO_STRING = "string"
GO_NULLSTRING = "sql.NullString"

GO_NULLTYPES = {
    GO_BOOL: GO_NULLBOOL,
    GO_INT64: GO_NULLINT64,
    GO_INT32: GO_NULLINT32,
    GO_FLOAT64: GO_NULLFLOAT64,
    GO_DATETIME: GO_NULLDATETIME,
    GO_STRING: GO_NULLSTRING,
}

GO_NULLTYPES_VALUES = {
    GO_BOOL: 'Bool',
    GO_INT64: 'Int64',
    GO_INT32: 'Int32',
    GO_FLOAT64: 'Float64',
    GO_DATETIME: 'Time',
    GO_STRING: 'String',
}

def to_camelcase(word):
    return ''.join(x.capitalize() or '_' for x in word.split('_'))

class Field:
    """ Field encapsulates a Django Model field """

    def __init__(self, m: 'Model', f: fields.Field):
        self.model = m
        self.field = f

        # Raw field type
        self.rawtype: str = None

        # Struct member name
        self.goname = to_camelcase(f.name)
        self.pubname = self.goname

        # Struct member type
        self.gotype: str = None

        # Raw type member
        self.rawmember: str = None   # raw type member

        # getter, if defined, will be generated to return struct member
        self.getter: str = None

        # if relmodel is defined too, then getter will return that model instead
        self.relmodel: 'Model' = None

        # Internal flag, during processing it may change, and will alter member names
        self._public = True

        # Null setting
        self.null: bool = self.field.null

        # raw value in null struct
        self.nullvalue: str = None

        # Is this field an autofield
        self.autofield: bool = False

    def reference_package(self, package):
        self.model.reference_package(package)

    @property
    def db_column(self):
        _, column = self.field.get_attname_column()
        return column


    @property
    def related_model_goname(self):
        if self.model.app == self.relmodel.app:
            return self.relmodel.goname

        return "{}.{}".format(self.relmodel.app.label, self.relmodel.goname)


    @property
    def related_model_qsname(self):
        if self.model.app == self.relmodel.app:
            return self.relmodel.qsname

        return "{}.{}".format(self.relmodel.app.label, self.relmodel.qsname)


    def _get_type(self):
        f = self.field

        # Autofields are read-only
        if isinstance(f, fields.BigAutoField):
            self._public = False
            self.getter = "Get{}".format(self.goname)
            self.autofield = True
            return GO_INT64

        if isinstance(f, fields.AutoField):
            self._public = False
            self.getter = "Get{}".format(self.goname)
            self.autofield = True
            return GO_INT32

        if isinstance(f, models.ForeignKey):
            mm: Options = f.related_model._meta
            app = self.model.get_app(mm.app_label)
            if app.generate:
                if app != self.model.app:
                    self.reference_package(app.gomodule)
                self.relmodel = self.model.get_app(mm.app_label).get_model(mm.model_name)
                self._public = False
            f = mm.pk

        if isinstance(f, (fields.BooleanField, fields.NullBooleanField)):
            return GO_BOOL
        if isinstance(f, (fields.BigIntegerField, fields.BigAutoField)):
            return GO_INT64
        if isinstance(f, (fields.SmallIntegerField, fields.IntegerField, fields.AutoField)):
            return GO_INT32
        if isinstance(f, fields.FloatField):
            return GO_FLOAT64
        if isinstance(f, (fields.DateField, fields.DateTimeField, fields.TimeField)):
            self.reference_package("time")
            return GO_DATETIME
        if isinstance(f, (fields.CharField, fields.TextField)):
            return GO_STRING

        return None

    def setup(self):
        self.rawtype = self._get_type()
        if self.gotype is None:
            if self.null:
                self.reference_package("database/sql")
                self.gotype = GO_NULLTYPES.get(self.rawtype, self.rawtype)
            else:
                self.gotype = self.rawtype

        if self._public == False:
            self.goname = self.goname[:1].lower() + self.goname[1:]

        if self.rawmember is None:
            if self.null:
                self.rawmember = '{}.{}'.format(self.goname, GO_NULLTYPES_VALUES.get(self.rawtype, None))
            else:
                self.rawmember = self.goname

        if self.null:
            if self.nullvalue is None:
                self.nullvalue = GO_NULLTYPES_VALUES.get(self.rawtype, None)

        if self.relmodel and self.null == False:
            self.reference_package("fmt")



_model_template = """// AUTO-GENERATED file for Django model {{ model.label }}

package {{ model.app.label }}

import (
{%- for p in model.packages | sort %}
    {{ p | string -}}
{% endfor %}
)

// {{ model.goname }} mirrors model {{ model.label }}
type {{ model.goname }} struct {
    existsInDB bool
{% for field in model.concrete_fields %}
    {{ field.goname }} {{ field.gotype }}
{%- endfor %}
}

// {{ model.qsname }} represents a queryset for {{ model.label }}
type {{ model.qsname }} struct {
    condFragments []models.ConditionFragment
    order []string
}

func (qs {{ model.qsname }}) filter(c string, p interface{}) {{ model.qsname }} {
    qs.condFragments = append(
        qs.condFragments,
        &models.UnaryFragment{
            Frag: c,
            Param: p,
        },
    )
    return qs
}

{% for field in model.concrete_fields -%}

{% if field.relmodel -%}
// Get{{ field.pubname }} returns {{ field.related_model_goname }}
func ({{ receiver }} *{{ model.goname }}) Get{{ field.pubname }}(db *sql.DB) (*{{ field.related_model_goname }}, error) {
    return {{ field.related_model_qsname }}{{ "{}" }}.{{ field.relmodel.pk.pubname }}Eq({{ receiver }}.{{ field.rawmember}}).First(db)
}

// Set{{ field.pubname }} sets foreign key pointer to {{ field.related_model_goname }}
func ({{ receiver }} *{{ model.goname }}) Set{{ field.pubname }}(ptr *{{ field.related_model_goname }}) error {
{%- if field.null %}
    if ptr != nil {
        {{ receiver }}.{{ field.rawmember }} = ptr.{{ field.relmodel.pkvalue }}
        {{ receiver }}.{{ field.goname }}.Valid = true
    } else {
        {{ receiver }}.{{ field.goname }}.Valid = false
    }
{%- else %}
    if ptr != nil {
        {{ receiver }}.{{ field.goname }} = ptr.{{ field.relmodel.pkvalue }}
    } else {
        return fmt.Errorf("{{ model.goname }}.Set{{ field.pubname }}: non-null field received null value")
    }
{%- endif %}

    return nil
}

{% endif -%}
{% if field.getter -%}
// {{ field.getter }} returns {{ model.goname }}.{{ field.pubname }}
func ({{ receiver }} *{{ model.goname }}) {{ field.getter }}() {{ field.gotype }} {
    return {{ receiver }}.{{ field.goname }}
}

{% endif -%}

{%- if field.null -%}
// {{ field.pubname }}IsNull filters for {{ field.goname }} being null
func (qs {{ model.qsname }}) {{ field.pubname }}IsNull() {{ model.qsname }} {
    qs.condFragments = append(
        qs.condFragments,
        &models.ConstantFragment{
            Constant: `{{ field.db_column | string }} IS NULL`,
        },
    )
    return qs
}

// {{ field.pubname }}IsNotNull filters for {{ field.goname }} being not null
func (qs {{ model.qsname }}) {{ field.pubname }}IsNotNull() {{ model.qsname }} {
    qs.condFragments = append(
        qs.condFragments,
        &models.ConstantFragment{
            Constant: `{{ field.db_column | string }} IS NOT NULL`,
        },
    )
    return qs
}

{% endif -%}

{%- if field.relmodel -%}
// {{ field.pubname }}Eq filters for {{ field.goname }} being equal to argument
func (qs {{ model.qsname }}) {{ field.pubname }}Eq(v *{{ field.related_model_goname }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} =`, v.{{ field.relmodel.pkvalue }})
}

type in{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }} struct {
    qs {{ field.related_model_qsname }}
}

func (in *in{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }}) GetConditionFragment(c *models.PositionalCounter) (string, []interface{}) {
    s, p := in.qs.QueryId(c)

    return `{{ field.db_column | string }} IN (` + s + `)`, p
}


func (qs {{ model.qsname }}) {{ field.pubname }}In(oqs {{ field.related_model_qsname }}) {{ model.qsname }} {
    qs.condFragments = append(
        qs.condFragments,
        &in{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }}{
            qs: oqs,
        },
    )

    return qs
}


{% else -%}
// {{ field.pubname }}Eq filters for {{ field.goname }} being equal to argument
func (qs {{ model.qsname }}) {{ field.pubname }}Eq(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} =`, v)
}

// {{ field.pubname }}Ne filters for {{ field.goname }} being not equal to argument
func (qs {{ model.qsname }}) {{ field.pubname }}Ne(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} <>`, v)
}

// {{ field.pubname }}Lt filters for {{ field.goname }} being less than argument
func (qs {{ model.qsname }}) {{ field.pubname }}Lt(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} <`, v)
}

// {{ field.pubname }}Le filters for {{ field.goname }} being less than or equal to argument
func (qs {{ model.qsname }}) {{ field.pubname }}Le(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} <=`, v)
}

// {{ field.pubname }}Gt filters for {{ field.goname }} being greater than argument
func (qs {{ model.qsname }}) {{ field.pubname }}Gt(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} >`, v)
}

// {{ field.pubname }}Ge filters for {{ field.goname }} being greater than or equal to argument
func (qs {{ model.qsname }}) {{ field.pubname }}Ge(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} >=`, v)
}

{% endif -%}

// OrderBy{{ field.pubname }} sorts result by {{ field.pubname }} in ascending order
func (qs {{ model.qsname }}) OrderBy{{ field.pubname }}() {{ model.qsname }} {
    qs.order = append(qs.order, `{{ field.db_column | string }}`)

    return qs
}

// OrderBy{{ field.pubname }}Desc sorts result by {{ field.pubname }} in descending order
func (qs {{ model.qsname }}) OrderBy{{ field.pubname }}Desc() {{ model.qsname }} {
    qs.order = append(qs.order, `{{ field.db_column | string }} DESC`)

    return qs
}

{% endfor -%}

func (qs {{ model.qsname }}) GetConditionFragment(c *models.PositionalCounter) (string, []interface{}) {
    var conds []string
    var condp []interface{}

    for _, cond := range qs.condFragments {
        s, p := cond.GetConditionFragment(c)

        conds = append(conds, s)
        condp = append(condp, p...)
    }

    return strings.Join(conds, " AND "), condp
}

func (qs {{ model.qsname }}) whereClause(c *models.PositionalCounter) (string, []interface{}) {
    if len(qs.condFragments) == 0 {
        return "", nil
    }

    cond, params := qs.GetConditionFragment(c)

    return " WHERE " + cond, params
}

func (qs {{ model.qsname }}) orderByClause() string {
    if len(qs.order) == 0 {
        return ""
    }

    return " ORDER BY " + strings.Join(qs.order, ", ")
}

func (qs {{ model.qsname }}) queryFull() (string, []interface{}) {
    c := &models.PositionalCounter{}

    s, p := qs.whereClause(c)

    return `{{ select_stmt }}` + s + qs.orderByClause(), p
}

// QueryId returns statement and parameters suitable for embedding in IN clause
func (qs {{ model.qsname }}) QueryId(c *models.PositionalCounter) (string, []interface{}) {
    s, p := qs.whereClause(c)

    return `{{ select_id_stmt }}` + s, p
}

// All returns all rows matching queryset filters
func (qs {{ model.qsname }}) All(db *sql.DB) ([]*{{ model.goname }}, error) {
    s, p := qs.queryFull()

    rows, err := db.Query(s, p...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var ret []*{{ model.goname }}
    for rows.Next() {
        obj := {{ model.goname }}{{ "{existsInDB: true}" }}
        if err = rows.Scan({{ select_member_ptrs }}); err != nil {
            return nil, err
        }
        ret = append(ret, &obj)
    }

    return ret, nil
}

// First returns the first row matching queryset filters, others are discarded
func (qs {{ model.qsname }}) First(db *sql.DB) (*{{ model.goname }}, error) {
    s, p := qs.queryFull()

    row := db.QueryRow(s, p...)

    obj := {{ model.goname }}{{ "{existsInDB: true}" }}
    err := row.Scan({{ select_member_ptrs }})
    switch err {
    case nil:
        return &obj, nil
    case sql.ErrNoRows:
        return nil, nil
    default:
        return nil, err
    }

}

// insert operation
func ({{ receiver }} *{{ model.goname }}) insert(db *sql.DB) error {
{%- if model.auto_fields %}
    row := db.QueryRow(`{{ insert_stmt }}`, {{ insert_members }})

    if err := row.Scan({{ insert_autoptr_members }}); err != nil {
        return err
    }
{%- else %}
    _, err := db.Exec(`{{ insert_stmt }}`, {{ insert_members }})

    if err != nil {
        return err
    }
{%- endif %}

    {{ receiver }}.existsInDB = true

    return nil
}

// update operation
func ({{ receiver }} *{{ model.goname }}) update(db *sql.DB) error {
    _, err := db.Exec(`{{ update_stmt }}`, {{ update_members }})

    return err
}

// Save inserts or updates record
func ({{ receiver }} *{{ model.goname }}) Save(db *sql.DB) error {
    if {{ receiver }}.existsInDB {
        return {{ receiver }}.update(db)
    }

    return {{ receiver }}.insert(db)
}

// Delete removes row from database
func ({{ receiver }} *{{ model.goname }}) Delete(db *sql.DB) error {
    _, err := db.Exec(`{{ delete_stmt }}`, {{ receiver }}.{{ model.pk.goname }})

    {{ receiver }}.existsInDB = false

    return err
}

"""

class Model:
    """ Model encapsulates a Django model """

    def __init__(self, app: 'Application', m: models.Model):
        self.app = app
        self.model = m

        # referenced packages
        self.packages = {"strings", "database/sql", os.path.join(args.gomodule, 'models')}

        # This is the Go struct name
        self.goname = to_camelcase(self.model_name)

        # Queryset name for the Model
        self.qsname = "{}QS".format(self.goname)

        # All fields for the model
        self.fields: List[Field] = []

        # Concrete fields, i.e. which need a struct member
        self.concrete_fields: List[Field] = []

        # User concrete fields, i.e. which are updated in database
        self.user_fields: List[Field] = []

        # Auto concrete fields, they need to be read back upon insert
        self.auto_fields: List[Field] = []

        # PK field
        self.pk: Field = None

        # PK value public access
        self.pkvalue: str = None

        # escaped column-list for selects
        self.select_column_list: str = None

    @property
    def gofspath(self) -> pathlib.Path:
        return pathlib.Path(os.path.join(self.app.gofspath, '{}.go'.format(self.model_name)))

    @property
    def model_name(self):
        return self.model._meta.model_name

    @property
    def label(self):
        return self.model._meta.label

    @property
    def db_table(self):
        return self.model._meta.db_table

    def reference_package(self, package: str):
        self.packages.add(package)

    def setup(self):
        """ Setup model """

        options: Options = self.model._meta

        for f in options.get_fields():
            field = Field(self, f)
            field.setup()

            # Skip not supported fields (e.g. unknown type, non-concrete, reverse relation)
            if field.rawtype is None:
                continue

            self.fields.append(field)

            self.concrete_fields.append(field)

            if field.autofield:
                self.auto_fields.append(field)
            else:
                self.user_fields.append(field)

            if getattr(f, 'primary_key', False):
                if self.pk is not None:
                    raise RuntimeError("More than one PK detected on %s", self.model)
                self.pk = field

        if self.pk:
            if self.pkvalue is None:
                if self.pk.getter:
                    self.pkvalue = '{}()'.format(self.pk.getter)
                else:
                    self.pkvalue = self.pk.goname

    def get_app(self, label: str) -> 'Application':
        return self.app.get_app(label)

    def generate(self, tmpl: jinja2.Template):
        path = self.gofspath
        with path.open('w') as fh:

            receiver = self.goname[:1].lower()

            select_stmt = 'SELECT {} FROM "{}"'.format(
                ', '.join(["\"{}\"".format(f.db_column) for f in self.concrete_fields]),
                self.db_table,
            )
            select_member_ptrs = ', '.join(["&obj.{}".format(f.goname) for f in self.concrete_fields])
            select_id_stmt = 'SELECT "{}" FROM "{}"'.format(
                self.pk.db_column,
                self.db_table,
            )

            insert_stmt = 'INSERT INTO "{}" ({}) VALUES ({})'.format(
                self.db_table,
                ', '.join(["\"{}\"".format(f.db_column) for f in self.user_fields]),
                ', '.join(["${}".format(i+1) for i in range(len(self.user_fields))]),
            )
            if self.auto_fields:
                insert_stmt += ' RETURNING {}'.format(
                    ', '.join(["\"{}\"".format(f.db_column) for f in self.auto_fields]),
                )
            insert_members = ', '.join(["{}.{}".format(receiver, f.goname) for f in self.user_fields])
            insert_autoptr_members = ', '.join(["&{}.{}".format(receiver, f.goname) for f in self.auto_fields])

            update_stmt = 'UPDATE "{}" SET {} WHERE "{}" = {}'.format(
                self.db_table,
                ', '.join(["\"{}\" = ${}".format(self.user_fields[i].db_column, i+1) for i in range(len(self.user_fields))]),
                self.pk.db_column,
                "${}".format(len(self.user_fields) + 1),
            )
            update_members = ', '.join(["{}.{}".format(receiver, f.goname) for f in self.user_fields + [self.pk]])

            delete_stmt = 'DELETE FROM "{}" WHERE "{}" = $1'.format(
                self.db_table,
                self.pk.db_column,
            )

            fh.write(tmpl.render(
                model=self,
                receiver=receiver,

                select_stmt=select_stmt,
                select_member_ptrs=select_member_ptrs,
                select_id_stmt=select_id_stmt,

                insert_stmt=insert_stmt,
                insert_members=insert_members,
                insert_autoptr_members=insert_autoptr_members,

                update_stmt=update_stmt,
                update_members=update_members,

                delete_stmt=delete_stmt,
            ))


class Application:
    """ Encapsulates a Django application

    Stores Models, models are indexed by Django model_name
    """

    def __init__(self, apps: 'Apps', app: AppConfig):
        self.apps = apps
        self.app = app
        self.models: Mapping[str, Model] = dict()
        self.generate = False

        for djmodel in app.get_models():
            # do not process abstract models
            if djmodel._meta.abstract:
                continue

            model = Model(self, djmodel)
            self.models[model.model_name] = model


    @property
    def label(self):
        return self.app.label


    # This two should be in sync
    @property
    def gomodule(self) -> str:
        """ Represents go module path """
        return os.path.join(args.gomodule, 'models', self.label)


    @property
    def gofspath(self) -> pathlib.Path:
        """ Represents relative path on filesystem """
        return pathlib.Path(os.path.join('models', self.label))


    def setup(self):
        """ Setup Application """
        for _, model in self.models.items():
            model.setup()


    def get_app(self, label: str) -> 'Application':
        return self.apps.get_app(label)


    def get_model(self, model_name: str) -> Model:
        return self.models[model_name]


    def do_generate(self, tmpl: jinja2.Template):
        path = self.gofspath
        path.mkdir(parents=True, exist_ok=True)

        for _, model in self.models.items():
            model.generate(tmpl)


class Apps:
    """ Registry of apps

    An app is identified by its Django label
    https://docs.djangoproject.com/en/3.0/ref/applications/#django.apps.AppConfig.label
    """

    def __init__(self):
        self.apps: Mapping[str, Application] = dict()
        for djapp in apps.get_app_configs():
            app = Application(self, djapp)
            self.apps[app.label] = app

    def generate(self, tmpl: jinja2.Template, apps: List[str]):
        # Mark apps to be generated
        for label in apps:
            app = self.apps[label]
            app.generate = True

        self._setup()

        for label in apps:
            app = self.apps[label]
            app.do_generate(tmpl)


    def _setup(self):
        for _, app in self.apps.items():
            if app.generate:
                app.setup()


    def get_app(self, label: str) -> Application:
        return self.apps[label]


if __name__ == '__main__':
    import sys, os
    sys.path.insert(0, os.getcwd())

    import django
    django.setup()

    parser = argparse.ArgumentParser()
    parser.add_argument("applications", nargs='+', type=str, help="Applications whose models to be generated")
    parser.add_argument("--gomodule", type=str, required=False, help="Final Go module path")

    args = parser.parse_args()

    jenv = jinja2.Environment()
    jenv.filters['string'] = lambda x: "\"{}\"".format(x)
    tmpl = jenv.from_string(_model_template)

    apps = Apps()
    apps.generate(tmpl, args.applications)
