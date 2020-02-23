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

        # Filter function prefix
        self.filterprefix: str = self.goname

    def reference_package(self, package):
        self.model.reference_package(package)

    @property
    def db_column(self):
        _, column = self.field.get_attname_column()
        return column

    def _get_type(self):
        f = self.field

        if isinstance(f, fields.BigAutoField):
            self._public = False
            self.getter = "Get{}".format(self.goname)
            self.reference_package("database/sql")
            self.null = True
            return GO_INT64

        if isinstance(f, fields.AutoField):
            self._public = False
            self.getter = "Get{}".format(self.goname)
            self.reference_package("database/sql")
            self.null = True
            return GO_INT32

        if isinstance(f, models.ForeignKey):
            mm: Options = f.related_model._meta
            app = self.model.get_app(mm.app_label)
            if app.generate:
                if app != self.model.app:
                    self.reference_package(app.gomodule)
                self.getter = "Get{}".format(self.goname)
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
        if isinstance(f, fields.DateTimeField):
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


_model_template = """// AUTO-GENERATED file for Django model {{ model.label }}

package {{ model.app.label }}

import (
{%- for p in model.packages %}
    {{ p | string -}}
{% endfor %}
)

// {{ model.goname }} mirrors model {{ model.label }}
type {{ model.goname }} struct {
{%- for field in model.concrete_fields %}
    {{ field.goname }} {{ field.gotype }}
{%- endfor %}
}

// {{ model.qsname }} represents a queryset for {{ model.label }}
type {{ model.qsname }} struct {
    conds []string
    condp []interface{}
}

func (qs {{ model.qsname }}) filter(c string, p interface{}) {{ model.qsname }} {
    qs.condp = append(qs.condp, p)
    qs.conds = append(qs.conds, fmt.Sprintf("%s $%d", c, len(qs.condparam)))
    return qs
}

{% for field in model.concrete_fields -%}
{% if field.null -%}
// {{ field.filterprefix }}IsNull filters for {{ field.goname }} being null
func (qs {{ model.qsname }}) {{ field.filterprefix }}IsNull() {{ model.qsname }} {
    qs.conds = append(qs.conds, `{{ field.db_column | string }} IS NULL`)
    return qs
}

// {{ field.filterprefix }}IsNotNull filters for {{ field.goname }} being not null
func (qs {{ model.qsname }}) {{ field.filterprefix }}IsNotNull() {{ model.qsname }} {
    qs.conds = append(qs.conds, `{{ field.db_column | string }} IS NOT NULL`)
    return qs
}
{%- endif %}

{%- if not field.relmodel -%}
// {{ field.filterprefix }}Eq filters for {{ field.goname }} being equal to argument
func (qs {{ model.qsname }}) {{ field.filterprefix }}Eq(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} =`, v)
}

// {{ field.filterprefix }}Ne filters for {{ field.goname }} being not equal to argument
func (qs {{ model.qsname }}) {{ field.filterprefix }}Ne(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} <>`, v)
}

// {{ field.filterprefix }}Lt filters for {{ field.goname }} being less than argument
func (qs {{ model.qsname }}) {{ field.filterprefix }}Lt(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} <`, v)
}

// {{ field.filterprefix }}Le filters for {{ field.goname }} being less than or equal to argument
func (qs {{ model.qsname }}) {{ field.filterprefix }}Le(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} <=`, v)
}

// {{ field.filterprefix }}Gt filters for {{ field.goname }} being greater than argument
func (qs {{ model.qsname }}) {{ field.filterprefix }}Gt(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} >`, v)
}

// {{ field.filterprefix }}Ge filters for {{ field.goname }} being greater than or equal to argument
func (qs {{ model.qsname }}) {{ field.filterprefix }}Ge(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} >=`, v)
}
{%- endif %}
{%- endfor %}

func (qs {{ model.qsname }}) queryString() string {
    var ret string = `SELECT {{ model.select_column_list }} FROM {{ model.db_table | string }}`

    if len(qs.conds) > 0 {
        ret = ret + " WHERE " + strings.Join(qs.conds, " AND ")        
    }

    return ret
}

"""

class Model:
    """ Model encapsulates a Django model """

    def __init__(self, app: 'Application', m: models.Model):
        self.app = app
        self.model = m

        # referenced packages
        self.packages = {"fmt", "database/sql"}

        # This is the Go struct name
        self.goname = to_camelcase(self.model_name)

        # Queryset name for the Model
        self.qsname = "{}QS".format(self.goname)

        # All fields for the model
        self.fields: List[Field] = []
        # Concrete fields, i.e. which need a struct member
        self.concrete_fields: List[Field] = []

        # PK field
        self.pk: Field = None

        # Non-pk fields
        self.nonpk_fields = []

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

            if field.rawtype is None:
                continue

            self.fields.append(field)
            if field.rawtype is not None:
                self.concrete_fields.append(field)

            if getattr(f, 'primary_key', False):
                if self.pk is not None:
                    raise RuntimeError("More than one PK detected on %s", self.model)
                self.pk = field
            else:
                self.nonpk_fields.append(field)

        self.select_column_list = ', '.join(["\"{}\"".format(f.db_column) for f in self.concrete_fields])

    def get_app(self, label: str) -> 'Application':
        return self.app.get_app(label)

    def generate(self, tmpl: jinja2.Template):
        path = self.gofspath
        with path.open('w') as fh:
            fh.write(tmpl.render(model=self))

    def _gen_field_filter_op(self, fh, f: Field, name, op, help):
        fnPrefix = "{}".format(f.goname.capitalize())

        fh.write("// {}{} filters {} to be {}\n".format(
            fnPrefix, name, f.goname, help
        ))
        fh.write("func (qs {}) {}{}(v {}) {} {{\n".format(
            self.qsname, fnPrefix, name, f.rawtype, self.qsname,
        ))
        fh.write("\tqs.condparam = append(qs.condparam, v)\n")
        fh.write("\tmarker := fmt.Sprintf(\"$%d\", len(qs.condparam))\n")
        fh.write("\tqs.condstr = append(qs.condstr, \"{} {} \" + marker)\n".format(
            f.db_column, op,
        ))
        fh.write("\treturn qs\n")
        fh.write("}\n\n")


    def gen_field_filters(self, fh, f: Field):
        fnPrefix = "{}".format(f.goname.capitalize())

        if f.null:
            fh.write("// {}IsNull filters {} to be NULL\n".format(
                fnPrefix, f.goname,
            ))
            fh.write("func (qs {}) {}IsNull() {} {{\n".format(
                self.qsname, fnPrefix, self.qsname,
            ))
            fh.write("\tqs.condstr = append(qs.condstr, \"{} IS NULL\")\n".format(
                f.db_column
            ))
            fh.write("\treturn qs\n")
            fh.write("}\n\n")

            fh.write("// {}IsNotNull filters {} NOT to be NULL\n".format(
                fnPrefix, f.goname,
            ))
            fh.write("func (qs {}) {}IsNotNull() {} {{\n".format(
                self.qsname, fnPrefix, self.qsname,
            ))
            fh.write("\tqs.condstr = append(qs.condstr, \"{} IS NOT NULL\")\n".format(
                f.db_column
            ))
            fh.write("\treturn qs\n")
            fh.write("}\n\n")

        self._gen_field_filter_op(fh, f, "Eq", "=", "equal to argument")
        self._gen_field_filter_op(fh, f, "Lt", "<", "less than argument")
        self._gen_field_filter_op(fh, f, "Le", "<=", "less than or equal to argument")
        self._gen_field_filter_op(fh, f, "Gt", ">", "greater than argument")
        self._gen_field_filter_op(fh, f, "Ge", ">=", "greater than or equal to argument")

    def gen_qs(self, fh):
        fh.write("type {} struct {{\n".format(self.qsname))
        fh.write("\tcondstr []string\n")
        fh.write("\tcondparam []interface{}\n")
        fh.write("}\n\n")

        concrete_fields = [f for f in self.fields if f.gotype is not None]

        for f in concrete_fields:
            self.gen_field_filters(fh, f)

        columns = ', '.join([f.db_column for f in concrete_fields])

        fh.write("func (qs {}) columns() string {{\n".format(self.qsname))
        fh.write("\treturn \"{}\"\n".format(columns))
        fh.write("}\n\n")

        fh.write("func (qs {}) queryString() string {{\n".format(self.qsname))
        fh.write("\tvar s string = \"SELECT {} FROM {}\"\n".format(columns, self.db_table))
        fh.write("\n")
        fh.write("\tif len(qs.condstr) > 0 {\n")
        fh.write("\t\ts = s + \" WHERE \" + strings.Join(qs.condstr, \" AND \")\n")
        fh.write("\t}\n")
        fh.write("\n")
        fh.write("\treturn s\n")
        fh.write("}\n\n")

        # Query functions
        fh.write("// All returns all matching records\n")
        fh.write("func (qs {}) All(db *sql.DB) ([]*{}, error) {{\n".format(
            self.qsname, self.goname))
        fh.write("\trows, err := db.Query(qs.queryString(), qs.condparam...)\n")
        fh.write("\tif err != nil {\n")
        fh.write("\t\treturn nil, err\n")
        fh.write("\t}\n")
        fh.write("\tdefer rows.Close()\n")
        fh.write("\n")
        fh.write("\tvar ret []*{}\n".format(self.goname))
        fh.write("\tfor rows.Next() {\n")
        fh.write("\t\tobj := {}{{}}\n".format(self.goname))
        fh.write("\t\tif err = rows.Scan(")
        fh.write(', '.join(["&obj.{}".format(f.goname) for f in concrete_fields]))
        fh.write("); err != nil {\n")
        fh.write("\t\t\treturn nil, err\n")
        fh.write("\t\t}\n")
        fh.write("\t\tret = append(ret, &obj)\n")
        fh.write("\t}\n")
        fh.write("\n")
        fh.write("\treturn ret, nil\n")
        fh.write("}\n\n")

        fh.write("// First returns the first matching record, discarding others if any\n")
        fh.write("func (qs {}) First(db *sql.DB) (*{}, error) {{\n".format(
            self.qsname, self.goname))
        fh.write("\trow := db.QueryRow(qs.queryString(), qs.condparam...)\n")
        fh.write("\tobj := {}{{}}\n".format(self.goname))
        fh.write("\tif err := row.Scan(")
        fh.write(', '.join(["&obj.{}".format(f.goname) for f in concrete_fields]))
        fh.write("); err != nil {\n")
        fh.write("\t\treturn nil, err\n")
        fh.write("\t}\n")
        fh.write("\n")
        fh.write("\treturn &obj, nil\n")
        fh.write("}\n\n")


    def gen_model(self, fh):
        fh.write("// {} model\n".format(self.goname))
        fh.write("type {} struct {{\n".format(self.goname))

        r = self.goname[:1].lower()

        for f in self.fields:
            fh.write("\t// {}\n".format(f.goname))

            if f.gotype:

                fh.write("\t")

                fh.write("{} {} ".format(f.goname, f.gotype))

                fh.write("\n")

        fh.write("}\n\n")

        # Generate queryset
        self.gen_qs(fh)

        for f in self.fields:
            if f.getter and f.relmodel is None:
                fh.write("func ({} *{}) {}() {} {{\n".format(
                    r, self.goname, f.getter, f.gotype
                ))
                fh.write("\treturn {}.{}\n".format(r, f.goname))
                fh.write("}\n\n")

        for f in self.fields:
            if f.getter and f.relmodel is not None:
                fh.write("func ({} *{}) {}(db *sql.DB) (*{}, error) {{\n".format(
                    r, self.goname, f.getter, f.relmodel.goname
                ))
                if f.field.null:
                    fh.write("\tif !{}.{}.Valid {{\n".format(r, f.goname))
                    fh.write("\t\treturn nil, nil\n")
                    fh.write("\t}\n\n")

                    fh.write("\treturn {}{{}}.{}Eq({}.{}.{}).First(db)\n".format(
                        f.relmodel.qsname, f.relmodel.pk.goname.capitalize(), r, f.goname,
                        GO_NULLTYPES_VALUES[f.gotype],
                    ))
                else:
                    fh.write("\treturn {}{{}}.{}Eq({}.{}).First(db)\n".format(
                        f.relmodel.qsname, f.relmodel.pk.goname.capitalize(), r, f.goname
                    ))

                fh.write("}\n\n")


class Generator:
    def __init__(self, app: str, model: str=None):
        self._app = apps.get_app_config(app)
        self._model = model
        self.path = app
        self.models = dict()

    def gen_models(self):
        if self._model:
            models = [self._app.get_model(self._model)]
        else:
            models = self._app.get_models()

        models = [GoModel(m) for m in models]

        for m in models:
            self.models[m.django_model_name] = m

        for m in models:
            m.parse_model(self.models)

        packages = {"strings", "fmt", "time"}
        for m in models:
            m.get_packages(packages)

        self._m = open('{}/models.go'.format(self.path), 'w')

        self._write_package(self._m, packages)

        for m in models:
            m.gen_model(self._m)

        self._m.close()

    def _write_package(self, fh, packages):
        fh.write("// Auto-generated file for {}\n\n".format(self._app))
        fh.write("package {}\n\n".format(self.path))
        fh.write("import (\n")
        for p in packages:
            fh.write("\t\"{}\"\n".format(p))
        fh.write(")\n\n")

        fh.write("var _ = time.Now\n\n")



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
