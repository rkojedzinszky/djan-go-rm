#!/usr/bin/env python

import os
import argparse
import pathlib
import shutil
import subprocess
from typing import List, Mapping

import jinja2
from django.apps import AppConfig, apps
from django.db import models
from django.db.models import fields
from django.db.models.options import Options
from django.contrib.postgres import fields as pgfields

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
        self.pubname: str = None

        # Struct member type
        self.gotype: str = None

        # Raw type member
        self.rawmember: str = None   # raw type member

        # getter, if defined, will be generated to return struct member
        self.getter: str = None

        # if relmodel is defined too, then getter will return that model instead
        self.relmodel: 'Model' = None

        # if reverse is defined, then the field is virtual, a reverse relation is in place
        # and a queryset will be returned
        self.reverse = False

        # Internal flag, during processing it may change, and will alter member names
        self._public = True

        # Null setting
        self.null: bool = self.field.null

        # raw value in null struct
        self.nullvalue: str = None

        # Is this field an autofield
        self.autofield: bool = False

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

    @property
    def remote_field(self) -> 'Field':
        if self.field.remote_field:
            return self.relmodel.get_field_by_raw_name(self.field.remote_field.name)

        return None

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

        if isinstance(f, (models.ForeignKey, models.ManyToOneRel)):
            mm: Options = f.related_model._meta
            app = self.model.get_app(mm.app_label)
            if app.generate:
                self.relmodel = self.model.get_app(mm.app_label).get_model(mm.model_name)
                self._public = False
                if isinstance(f, models.ForeignKey):
                    self.getter = "Get{}Raw".format(self.goname)
                else:  # models.ManyToOneRel
                    if app == self.model.app:
                        self.getter = self.goname
                        self.reverse = True
                    return None

                if app != self.model.app:
                    self.model.model_packages.add(app.gomodule)

            if isinstance(f, models.ManyToOneRel):
                return None

            f = mm.pk

        # many-to-many relations not supported
        if isinstance(f, (models.ManyToManyField, models.ManyToManyRel)):
            return None

        # Array support for basic types
        arrayprefix = ''
        while isinstance(f, pgfields.ArrayField):
            arrayprefix = '[]' + arrayprefix
            f = f.base_field

        return arrayprefix + self._get_type_basic(f)

    def _get_type_basic(self, f):
        if isinstance(f, (fields.BooleanField, fields.NullBooleanField)):
            return GO_BOOL
        if isinstance(f, (fields.BigIntegerField, fields.BigAutoField)):
            return GO_INT64
        if isinstance(f, (fields.SmallIntegerField, fields.IntegerField, fields.AutoField)):
            return GO_INT32
        if isinstance(f, fields.FloatField):
            return GO_FLOAT64
        if isinstance(f, (fields.DateField, fields.DateTimeField, fields.TimeField)):
            self.model.core_packages.add("time")
            return GO_DATETIME

        return GO_STRING

    def setup(self):
        if self.goname.lower() == 'id':
            self.goname = 'ID'

        self.pubname = self.goname

        self.rawtype = self._get_type()
        if self.rawtype is None:
            return

        if self.gotype is None:
            if self.null:
                self.model.core_packages.add("database/sql")
                self.gotype = GO_NULLTYPES.get(self.rawtype, self.rawtype)
            else:
                self.gotype = self.rawtype

        if not self._public:
            if self.goname.lower() == 'id':
                self.goname = 'id'
            else:
                self.goname = self.goname[:1].lower() + self.goname[1:]

        if self.rawmember is None:
            if self.null:
                self.rawmember = '{}.{}'.format(self.goname, GO_NULLTYPES_VALUES.get(self.rawtype, None))
            else:
                self.rawmember = self.goname

        if self.null:
            if self.nullvalue is None:
                self.nullvalue = GO_NULLTYPES_VALUES.get(self.rawtype, None)


_model_template = """
// Code generated for Django model {{ model.label }}. DO NOT EDIT.

/*
  Command used to generate:

  {{ model.app.apps.commandline }}

  https://github.com/rkojedzinszky/djan-go-rm
*/

package {{ model.app.label }}

import (
{% for p in model.core_packages %}
    {{ p | string -}}
{% endfor %}

{% for p in model.external_packages %}
    {{ p | string -}}
{% endfor %}

{% for p in model.model_packages %}
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

// {{ model.goname }}List is a list of {{ model.goname }}
type {{ model.goname }}List []*{{ model.goname }}

// {{ model.qsname }} represents a queryset for {{ model.label }}
type {{ model.qsname }} struct {
    condFragments models.AndFragment
    order []string
    forClause string
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

// Or combines given expressions with OR operator
func (qs {{ model.qsname }}) Or(exprs ...{{ model.qsname }}) {{ model.qsname }} {
    var o models.OrFragment

    for _, expr := range exprs {
        o = append(o, expr.condFragments)
    }

    qs.condFragments = append(
        qs.condFragments,
        o,
    )

    return qs
}

{% for field in model.concrete_fields -%}

{% if field.relmodel -%}
// Get{{ field.pubname }} returns {{ field.related_model_goname }}
func ({{ receiver }} *{{ model.goname }}) Get{{ field.pubname }}(ctx context.Context, db models.DBInterface) (*{{ field.related_model_goname }}, error) {
{%- if field.null %}
    if !{{ receiver }}.{{ field.goname }}.Valid {
        return nil, nil
    }
{% endif %}
    return {{ field.related_model_qsname }}{{ "{}" }}.{{ field.relmodel.pk.pubname }}Eq({{ receiver }}.{{ field.rawmember}}).First(ctx, db)
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

// {{ field.pubname }}RawEq filters for {{ field.goname }} being equal to raw argument
func (qs {{ model.qsname }}) {{ field.pubname }}RawEq(v {{ field.rawtype }}) {{ model.qsname }} {
    return qs.filter(`{{ field.db_column | string }} =`, v)
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

{% if field.rawtype != "bool" %}
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
{% endif %}

type in{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }} []interface{}

func (in in{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }}) GetConditionFragment(c *models.PositionalCounter) (string, []interface{}) {
    if len(in) == 0 {
        return `false`, nil
    }

    var params []string
    for range in {
        params = append(params, c.Get())
    }

    return `{{ field.db_column | string }} IN (` + strings.Join(params, ", ") + `)`, in
}

func (qs {{ model.qsname }}) {{ field.pubname }}In(values []{{ field.rawtype }}) {{ model.qsname }} {
    var vals []interface{}
    for _, v := range values {
        vals = append(vals, v)
    }

    qs.condFragments = append(
        qs.condFragments,
        in{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }}(vals),
    )

    return qs
}

type notin{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }} []interface{}

func (in notin{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }}) GetConditionFragment(c *models.PositionalCounter) (string, []interface{}) {
    if len(in) == 0 {
        return `false`, nil
    }

    var params []string
    for range in {
        params = append(params, c.Get())
    }

    return `{{ field.db_column | string }} NOT IN (` + strings.Join(params, ", ") + `)`, in
}

func (qs {{ model.qsname }}) {{ field.pubname }}NotIn(values []{{ field.rawtype }}) {{ model.qsname }} {
    var vals []interface{}
    for _, v := range values {
        vals = append(vals, v)
    }

    qs.condFragments = append(
        qs.condFragments,
        notin{{ model.goname }}{{ field.goname }}{{ field.relmodel.goname }}(vals),
    )

    return qs
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

// OrderByRandom randomizes result
func (qs {{ model.qsname }}) OrderByRandom() {{ model.qsname }} {
    qs.order = append(qs.order, `random()`)

    return qs
}

// ForUpdate marks the queryset to use FOR UPDATE clause
func (qs {{ model.qsname }}) ForUpdate() {{ model.qsname }} {
    qs.forClause = " FOR UPDATE"

    return qs
}

// ForUpdateNowait marks the queryset to use FOR UPDATE NOWAIT clause
func (qs {{ model.qsname }}) ForUpdateNowait() {{ model.qsname }} {
    qs.forClause = " FOR UPDATE NOWAIT"

    return qs
}

// ForUpdateSkipLocked marks the queryset to use FOR UPDATE SKIP LOCKED clause
func (qs {{ model.qsname }}) ForUpdateSkipLocked() {{ model.qsname }} {
    qs.forClause = " FOR UPDATE SKIP LOCKED"

    return qs
}

// ClearForUpdate clears FOR UPDATE clause set on queryset
func (qs {{ model.qsname }}) ClearForUpdate() {{ model.qsname }} {
    qs.forClause = ""

    return qs
}

func (qs {{ model.qsname }}) whereClause(c *models.PositionalCounter) (string, []interface{}) {
    if len(qs.condFragments) == 0 {
        return "", nil
    }

    cond, params := qs.condFragments.GetConditionFragment(c)

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
    s += qs.orderByClause()
    s += qs.forClause

    return `{{ select_stmt }}` + s, p
}

// QueryId returns statement and parameters suitable for embedding in IN clause
func (qs {{ model.qsname }}) QueryId(c *models.PositionalCounter) (string, []interface{}) {
    s, p := qs.whereClause(c)

    return `{{ select_id_stmt }}` + s, p
}

// Count returns the number of rows matching queryset filters
func (qs {{ model.qsname }}) Count(ctx context.Context, db models.DBInterface) (count int, err error) {
    c := &models.PositionalCounter{}

    s, p := qs.whereClause(c)

    row := db.QueryRow(ctx, `{{ select_count_stmt }}` + s, p...)

    err = row.Scan(&count)

    return
}

// All returns all rows matching queryset filters
func (qs {{ model.qsname }}) All(ctx context.Context, db models.DBInterface) ({{ model.goname }}List, error) {
    s, p := qs.queryFull()

    rows, err := db.Query(ctx, s, p...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var ret {{ model.goname }}List
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
func (qs {{ model.qsname }}) First(ctx context.Context, db models.DBInterface) (*{{ model.goname }}, error) {
    s, p := qs.queryFull()

    s += " LIMIT 1"

    row := db.QueryRow(ctx, s, p...)

    obj := {{ model.goname }}{{ "{existsInDB: true}" }}
    err := row.Scan({{ select_member_ptrs }})
    switch err {
    case nil:
        return &obj, nil
    case pgx.ErrNoRows:
        return nil, nil
    default:
        return nil, err
    }
}

// Delete deletes rows matching queryset filters
func (qs {{ model.qsname }}) Delete(ctx context.Context, db models.DBInterface) (int64, error) {
    c := &models.PositionalCounter{}

    s, p := qs.whereClause(c)
    s = `{{ delete_qs_stmt }}` + s

    result, err := db.Exec(ctx, s, p...)
    if err != nil {
        return 0, err
    }

    return result.RowsAffected(), nil
}

// Update returns an Update queryset inheriting all the filter conditions, which then can be
// used to specify columns to be updated. At the end, .Exec() must be called to do the real operation.
func (qs {{ model.qsname }}) Update() {{ model.uqsname }} {
    return {{ model.uqsname }}{{ "{condFragments: qs.condFragments}" }}
}

// {{ model.uqsname }} represents an updated queryset for {{ model.label }}
type {{ model.uqsname }} struct {
    updates       []models.ConditionFragment
    condFragments []models.ConditionFragment
}

func (uqs {{ model.uqsname }}) update(c string, v interface{}) {{ model.uqsname }} {
    var frag models.ConditionFragment

    if v == nil {
        frag = &models.ConstantFragment{
            Constant: c + " = NULL",
        }
    } else {
        frag = &models.UnaryFragment{
            Frag: c + " =",
            Param: v,
        }
    }

    uqs.updates = append(uqs.updates, frag)

    return uqs
}

{% for field in model.concrete_fields -%}

{% if field.relmodel -%}
// Set{{ field.pubname }} sets foreign key pointer to {{ field.related_model_goname }}
func (uqs {{ model.uqsname }}) Set{{ field.pubname }}(ptr *{{ field.related_model_goname }}) {{ model.uqsname }} {
    if ptr != nil {
        return uqs.update(`{{ field.db_column | string }}`, ptr.{{ field.relmodel.pkvalue }})
    }

    return uqs.update(`{{ field.db_column | string }}`, nil)
}

{%- else -%}

// Set{{ field.pubname }} sets {{ field.pubname }} to the given value
func (uqs {{ model.uqsname }}) Set{{ field.pubname }}(v {{ field.gotype }}) {{ model.uqsname }} {
    return uqs.update(`{{ field.db_column | string }}`, v)
}

{% endif -%}

{% endfor -%}

// Exec executes the update operation
func (uqs {{ model.uqsname }}) Exec(ctx context.Context, db models.DBInterface) (int64, error) {
    if len(uqs.updates) == 0 {
        return 0, nil
    }

    c := &models.PositionalCounter{}

    var params []interface{}

    var sets []string
    for _, set := range uqs.updates {
        s, p := set.GetConditionFragment(c)

        sets = append(sets, s)
        params = append(params, p...)
    }

    ws, wp := {{ model.qsname }}{{ "{condFragments: uqs.condFragments}" }}.whereClause(c)

    st := `{{ update_qs_stmt }}` + strings.Join(sets, ", ") + ws

    params = append(params, wp...)

    result, err := db.Exec(ctx, st, params...)
    if err != nil {
        return 0, err
    }

    return result.RowsAffected(), nil
}

// insert operation
func ({{ receiver }} *{{ model.goname }}) insert(ctx context.Context, db models.DBInterface) error {
{%- if model.auto_fields %}
    row := db.QueryRow(ctx, `{{ insert_stmt }}`, {{ insert_members }})

    if err := row.Scan({{ insert_autoptr_members }}); err != nil {
        return err
    }
{%- else %}
    _, err := db.Exec(ctx, `{{ insert_stmt }}`, {{ insert_members }})

    if err != nil {
        return err
    }
{%- endif %}

    {{ receiver }}.existsInDB = true

    return nil
}

// update operation
func ({{ receiver }} *{{ model.goname }}) update(ctx context.Context, db models.DBInterface) error {
    _, err := db.Exec(ctx, `{{ update_stmt }}`, {{ update_members }})

    return err
}

// Save inserts or updates record
func ({{ receiver }} *{{ model.goname }}) Save(ctx context.Context, db models.DBInterface) error {
    if {{ receiver }}.existsInDB {
        return {{ receiver }}.update(ctx, db)
    }

    return {{ receiver }}.insert(ctx, db)
}

// Delete removes row from database
func ({{ receiver }} *{{ model.goname }}) Delete(ctx context.Context, db models.DBInterface) error {
    _, err := db.Exec(ctx, `{{ delete_stmt }}`, {{ receiver }}.{{ model.pk.goname }})

    {{ receiver }}.existsInDB = false

    return err
}

// Save saves all elements, optimizing inserts in a batch
func ({{ receiver }}l {{ model.goname }}List)Save(ctx context.Context, db models.DBInterface) error {
    var inserts {{ model.goname }}List

    for _, {{ receiver }} := range {{ receiver }}l {
        if {{ receiver }}.existsInDB {
            if err := {{ receiver }}.update(ctx, db); err != nil {
                return err
            }
        } else {
            inserts = append(inserts, {{ receiver }})
        }
    }

    if len(inserts) == 0 {
        return nil
    }

    vva := make([]string, 0, len(inserts))
    vaa := make([]any, 0, {{ insert_stmt_column_count }} * len(inserts))
    offs := 1
    for _, {{ receiver }} := range inserts {
        vva = append(vva, {{ insert_stmt_values_template }})
        vaa = append(vaa, {{ insert_members }})
        offs += {{ insert_stmt_column_count }}
    }

    qs := `{{ batch_insert_stmt}} ` + strings.Join(vva, ", "){%- if model.auto_fields %} + ` {{ batch_insert_returning }}`{% endif %}

{%- if model.auto_fields %}
    rows, err := db.Query(ctx, qs, vaa...)
{%- else %}
    _, err := db.Exec(ctx, qs, vaa...)
{%- endif %}

    if err != nil {
        return err
    }

{%- if model.auto_fields %}
    defer rows.Close()
{% endif %}

    for _, {{ receiver }} := range inserts {
{%- if model.auto_fields %}
        if !rows.Next() {
            return rows.Err()
        }

        if err := rows.Scan({{ insert_autoptr_members }}); err != nil {
            return err
        }
{% endif %}
        {{ receiver }}.existsInDB = true
    }

    return nil
}

{% for field in model.reverse_fields -%}
// {{ field.getter }} returns the set of {{ field.relmodel.goname }} referencing this {{ model.goname }} instance
func ({{ receiver }} *{{ model.goname }}) {{ field.getter }}() {{ field.related_model_qsname }} {
    return {{ field.related_model_qsname }}{{ "{}" }}.{{ field.remote_field.pubname }}Eq({{receiver}})
}

{% endfor %}

"""


class Model:
    """ Model encapsulates a Django model """

    def __init__(self, app: 'Application', m: models.Model):
        self.app = app
        self.model = m

        # referenced packages
        self.core_packages = {"context", "fmt", "strings"}
        self.external_packages = {"github.com/jackc/pgx/v5"}
        self.model_packages = {os.path.join(args.gomodule, 'models')}

        # This is the Go struct name
        self.goname = to_camelcase(self.model_name)

        # Queryset name for the Model
        self.qsname = "{}QS".format(self.goname)

        # Update queryset name for the Model
        self.uqsname = "{}UpdateQS".format(self.goname)

        # All fields for the model
        self.fields: List[Field] = []

        # Concrete fields, i.e. which need a struct member
        self.concrete_fields: List[Field] = []

        # User concrete fields, i.e. which are updated in database
        self.user_fields: List[Field] = []

        # Auto concrete fields, they need to be read back upon insert
        self.auto_fields: List[Field] = []

        # Reverse fields
        self.reverse_fields: List[Field] = []

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

    def get_field_by_raw_name(self, name: str) -> Field:
        for f in self.fields:
            if f.field.name == name:
                return f

        return None

    def setup(self):
        """ Setup model """

        options: Options = self.model._meta

        for f in options.get_fields():
            field = Field(self, f)
            field.setup()

            if field.reverse:
                self.reverse_fields.append(field)
                continue

            # Skip not supported fields (e.g. unknown type, non-concrete)
            if field.rawtype is None:
                continue

            self.fields.append(field)

            self.concrete_fields.append(field)

            is_pkey = getattr(f, 'primary_key', False)
            if is_pkey:
                if self.pk is not None:
                    raise RuntimeError("More than one PK detected on %s", self.model)
                self.pk = field

            if field.autofield:
                self.auto_fields.append(field)
            elif not is_pkey:
                self.user_fields.append(field)

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
        select_count_stmt = 'SELECT COUNT("{}") FROM "{}"'.format(
            self.pk.db_column,
            self.db_table,
        )

        insert_fields = [] + self.user_fields
        if not self.pk.autofield:
            insert_fields += [self.pk]

        batch_insert_stmt = 'INSERT INTO "{}" ({}) VALUES'.format(
            self.db_table,
            ', '.join(["\"{}\"".format(f.db_column) for f in insert_fields]),
        )
        insert_stmt = '{} ({})'.format(
            batch_insert_stmt,
            ', '.join(["${}".format(i + 1) for i in range(len(insert_fields))]),
        )
        insert_stmt_column_count = len(insert_fields)
        insert_stmt_values_template = 'fmt.Sprintf("({})", {})'.format(
            ', '.join(["$%d" for i in range(len(insert_fields))]),
            ', '.join(['offs + {}'.format(i) for i in range(len(insert_fields))]),
        )
        batch_insert_returning = ''
        if self.auto_fields:
            batch_insert_returning = 'RETURNING {}'.format(
                ', '.join(["\"{}\"".format(f.db_column) for f in self.auto_fields]),
            )
            insert_stmt += ' ' + batch_insert_returning
        insert_members = ', '.join(["{}.{}".format(receiver, f.goname) for f in insert_fields])
        insert_autoptr_members = ', '.join(["&{}.{}".format(receiver, f.goname) for f in self.auto_fields])

        update_stmt = 'UPDATE "{}" SET {} WHERE "{}" = {}'.format(
            self.db_table,
            ', '.join(["\"{}\" = ${}".format(self.user_fields[i].db_column, i + 1) for i in range(len(self.user_fields))]),
            self.pk.db_column,
            "${}".format(len(self.user_fields) + 1),
        )
        update_members = ', '.join(["{}.{}".format(receiver, f.goname) for f in self.user_fields + [self.pk]])

        delete_stmt = 'DELETE FROM "{}" WHERE "{}" = $1'.format(
            self.db_table,
            self.pk.db_column,
        )

        update_qs_stmt = 'UPDATE "{}" SET '.format(self.db_table)
        delete_qs_stmt = 'DELETE FROM "{}"'.format(self.db_table)

        with path.open('w') as fh:
            fh.write(tmpl.render(
                model=self,
                receiver=receiver,

                select_stmt=select_stmt,
                select_member_ptrs=select_member_ptrs,
                select_id_stmt=select_id_stmt,
                select_count_stmt=select_count_stmt,

                batch_insert_stmt=batch_insert_stmt,
                insert_stmt=insert_stmt,
                insert_stmt_column_count=insert_stmt_column_count,
                insert_stmt_values_template=insert_stmt_values_template,
                batch_insert_returning=batch_insert_returning,
                insert_members=insert_members,
                insert_autoptr_members=insert_autoptr_members,

                update_stmt=update_stmt,
                update_members=update_members,

                delete_stmt=delete_stmt,

                update_qs_stmt=update_qs_stmt,

                delete_qs_stmt=delete_qs_stmt,
            ))

        subprocess.check_call(["gofmt", "-w", path.as_posix()])


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

    def __init__(self, commandline: str = None):
        self.apps: Mapping[str, Application] = dict()
        self.commandline = commandline

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
    import sys
    import os
    sys.path.insert(0, os.getcwd())

    commandline = 'DJANGO_SETTINGS_MODULE={} {}'.format(os.getenv('DJANGO_SETTINGS_MODULE'), ' '.join(sys.argv))

    import django
    django.setup()

    parser = argparse.ArgumentParser()
    parser.add_argument("applications", nargs='+', type=str, help="Applications whose models to be generated")
    parser.add_argument("--gomodule", type=str, required=False, help="Final Go module path")

    args = parser.parse_args()

    jenv = jinja2.Environment()
    jenv.filters['string'] = lambda x: "\"{}\"".format(x)
    tmpl = jenv.from_string(_model_template)

    apps = Apps(commandline=commandline)
    apps.generate(tmpl, args.applications)

    # copy interface.go
    spath = pathlib.Path(__file__).parent / 'static' / 'interface.go'
    shutil.copy(spath.as_posix(), 'models')
