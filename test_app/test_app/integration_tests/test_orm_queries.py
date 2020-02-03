# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from django.db.models import Q
from django.test import TestCase

from dqp import execute_stmt, Placeholder, ListPlaceholder
from dqp.prepared_stmt_controller import PreparedStatementController
from dqp.queryset import PreparedStatementQuerySet
from dqp.exceptions import CannotAlterPreparedStatementQuerySet, PreparedQueryNotSupported

from test_app.models import Species, Animal, Items


class TestORMQueries(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.tiger = Species(name="Tiger")
        cls.tiger.save()
        cls.carp = Species(name="Carp")
        cls.carp.save()
        cls.crow = Species(name="Crow")
        cls.crow.save()

    def test_prepare_all(self):
        """
        Given an ORM query is prepared with the all() function
        When  the prepared statement is executed
        Then  all records from the model will be returned in a query set
        """

        def all_species():
            return Species.prepare.all().order_by("pk")

        PreparedStatementController().register_qs("all_species", all_species)
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)

        qs = execute_stmt("all_species")

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 3)
        self.assertTrue(isinstance(qs[0], Species))
        self.assertEqual(qs[0].name, self.tiger.name)

    def test_prepare_filter(self):
        """
        Given an ORM query is prepared with a filter
        And   the filter is a Placeholder
        When  the prepared statement is executed with a keyword argument for the filter
        Then  only the records which match the filter will be returned in a query set
        """

        def filter_species():
            return Species.prepare.filter(name=Placeholder("name"))

        PreparedStatementController().register_qs("filter_species", filter_species)
        PreparedStatementController().prepare_qs_stmt("filter_species", force=True)

        qs = execute_stmt("filter_species", name="Carp")

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 1)
        self.assertTrue(isinstance(qs[0], Species))
        self.assertEqual(qs[0].name, self.carp.name)

    def test_related_id_filter(self):
        """
        Given an ORM query is prepared with a filter on the id of a related model
        And   the filter is a Placeholder
        When  the prepared statement is executed with a keyword argument for the filter
        And   the filter is a related model id
        Then  only the records which match the filter will be returned in a query set
        """
        Animal.objects.update_or_create(name="Tigger", species=self.tiger)
        Animal.objects.update_or_create(name="Jack Daw", species=self.crow)
        Animal.objects.update_or_create(name="C. Orvid", species=self.crow)
        Animal.objects.update_or_create(name="Koi", species=self.carp)

        def filter_animals():
            return Animal.prepare.filter(species_id=Placeholder("species_id")).order_by("id")

        PreparedStatementController().register_qs("filter_animals", filter_animals)
        PreparedStatementController().prepare_qs_stmt("filter_animals", force=True)

        qs = execute_stmt("filter_animals", species_id=self.crow.id)

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 2)
        self.assertEqual(qs[0].name, "Jack Daw")
        self.assertEqual(qs[1].name, "C. Orvid")

    def test_doubly_related_id_filter(self):
        """
        Given an ORM query is prepared with a filter on the id of a related model
        And   the filter is a Placeholder
        When  the prepared statement is executed with a keyword argument for the filter
        And the filter is related by two foreign keys
        Then  only the records which match the filter will be returned in a query set
        """
        a1 = Animal.objects.create(name="Jack Daw", species=self.crow)
        a2 = Animal.objects.create(name="C. Orvid", species=self.crow)
        a3 = Animal.objects.create(name="Tigger", species=self.tiger)

        Items.objects.update_or_create(description="bird cage", animal=a1)
        Items.objects.update_or_create(description="whistle", animal=a1)
        Items.objects.update_or_create(description="bag of seeds", animal=a2)
        Items.objects.update_or_create(description="honey", animal=a3)

        def filter_items():
            return Items.prepare.filter(animal__species_id=Placeholder("species_id")).order_by("id")

        PreparedStatementController().register_qs("filter_items", filter_items)
        PreparedStatementController().prepare_qs_stmt("filter_items", force=True)

        qs = execute_stmt("filter_items", species_id=self.crow.id)

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 3)
        self.assertEqual(qs[0].description, "bird cage")
        self.assertEqual(qs[1].description, "whistle")
        self.assertEqual(qs[2].description, "bag of seeds")

    def test_related_filter(self):
        """
        Given an ORM query is prepared with a filter on a field in a related model
        And   the filter is a Placeholder
        When  the prepared statement is executed with a keyword argument for the filter
        And   the filter is a __ relation
        Then  only the records which match the filter will be returned in a query set
        """
        Animal.objects.update_or_create(name="Tigger", species=self.tiger)
        Animal.objects.update_or_create(name="Jack Daw", species=self.crow)
        Animal.objects.update_or_create(name="C. Orvid", species=self.crow)
        Animal.objects.update_or_create(name="Koi", species=self.carp)

        def filter_animals():
            # Note the use of the double underscore here compared to test_related_id_filter
            return Animal.prepare.filter(species__id=Placeholder("species_id")).order_by("id")

        PreparedStatementController().register_qs("filter_animals", filter_animals)
        PreparedStatementController().prepare_qs_stmt("filter_animals", force=True)

        qs = execute_stmt("filter_animals", species_id=self.crow.id)

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 2)
        self.assertEqual(qs[0].name, "Jack Daw")
        self.assertEqual(qs[1].name, "C. Orvid")


    def test_prepare_in(self):
        """
        Given an ORM query is prepared with an `__in` filter
        And   the filter is a ListPlaceholder
        When  the prepared statement is executed with a keyword argument which is a list for the filter
        Then  only the records which match the filter will be returned in a query set
        """

        def filter_species_in():
            return Species.prepare.filter(id__in=ListPlaceholder("ids")).order_by("id")

        PreparedStatementController().register_qs("filter_species_in", filter_species_in)
        PreparedStatementController().prepare_qs_stmt("filter_species_in", force=True)

        qs = execute_stmt("filter_species_in", ids=[self.carp.id, self.crow.id])

        self.assertEqual(len(qs), 2)
        self.assertEqual(qs[0].id, self.carp.id)
        self.assertEqual(qs[1].id, self.crow.id)

    def test_prepare_icontains(self):
        """
        Given an ORM query is prepared with an `__icontains` filter
        When  the prepared statement is executed with a keyword argument for the filter
        Then  only the records which contain the filter will be returned in a query set
        """

        def filter_species_like():
            return Species.prepare.filter(name__icontains=Placeholder("name"))

        PreparedStatementController().register_qs("filter_species_like", filter_species_like)
        PreparedStatementController().prepare_qs_stmt("filter_species_like", force=True)

        qs = execute_stmt("filter_species_like", name="car")

        self.assertEqual(len(qs), 1)
        self.assertTrue(isinstance(qs[0], Species))
        self.assertEqual(qs[0].name, self.carp.name)

    def test_filter_with_constant(self):
        """
        Given an ORM query is prepared with a filter that has no placeholders
        When  the prepared statement is executed without any keyword arguments
        Then  only the records which match the filter will be returned in a query set
        """

        def filter_species():
            return Species.prepare.filter(pk=self.crow.pk)

        PreparedStatementController().register_qs("filter_species", filter_species)
        PreparedStatementController().prepare_qs_stmt("filter_species", force=True)

        qs = execute_stmt("filter_species")

        self.assertEqual(qs[0].name, self.crow.name)

    def test_filter_wth_mixed_params(self):
        """
        Given an ORM query is prepared with a filter that has both placeholders and contant value filters
        When  the prepared statement is executed with a keyword arguments for placehlder filters
        Then  only the records which match alls filter will be returned in a query set
        """

        def filter_species():
            return Species.prepare.filter(Q(pk=self.crow.pk) | Q(pk=Placeholder("pk"))).order_by("pk")

        PreparedStatementController().register_qs("filter_species", filter_species)
        PreparedStatementController().prepare_qs_stmt("filter_species", force=True)

        qs = execute_stmt("filter_species", pk=self.tiger.pk)

        self.assertEqual(len(qs), 2)
        self.assertEqual(qs[0].name, self.tiger.name)
        self.assertEqual(qs[1].name, self.crow.name)

    def test_filter_not_enough_params(self):
        """
        Given an ORM query is prepared with a filter that has placeholders
        When  the prepared statement is executed without any keyword arguments
        Then  a ValueError will be raised
        And   the error message will be "Not enough parameters supplied to execute prepared statement"
        """

        def filter_species():
            return Species.prepare.filter(Q(pk=self.crow.pk) | Q(pk=Placeholder("pk")))

        PreparedStatementController().register_qs("filter_species", filter_species)
        PreparedStatementController().prepare_qs_stmt("filter_species", force=True)

        # And again with no params
        with self.assertRaises(ValueError) as ctx:
            qs = execute_stmt("filter_species")
        self.assertEqual(str(ctx.exception), "Not enough parameters supplied to execute prepared statement")

    def test_filter_missing_param(self):
        """
        Given an ORM query is prepared with a filter that has multiple placeholders
        When  the prepared statement is executed with some but not all keyword arguments
        Then  a ValueError will be raised
        And   the error message will be "Missing parameter {} is required to execute prepared statement"
        """

        def filter_species():
            return Species.prepare.filter(Q(pk=self.crow.pk) | Q(pk=Placeholder("pk")) | Q(pk=Placeholder("pk2")))

        PreparedStatementController().register_qs("filter_species", filter_species)
        PreparedStatementController().prepare_qs_stmt("filter_species", force=True)

        # And again with no params
        with self.assertRaises(ValueError) as ctx:
            qs = execute_stmt("filter_species", pk=1)
        self.assertEqual(str(ctx.exception), "Missing parameter pk2 is required to execute prepared statement")

    def test_all_params_have_unique_names(self):
        """
        Given an ORM query is created with a filter that has multiple placeholders with non-unique names
        When  the query is prepared
        Then  a NameError will be raised
        And   the error message will be "Repeated placeholder name: {}. All placeholders in a query must have unique names."
        """

        def filter_species():
            return Species.prepare.filter(Q(pk=Placeholder("pk")) | Q(pk=Placeholder("pk")))

        PreparedStatementController().register_qs("filter_species", filter_species)
        with self.assertRaises(NameError) as ctx:
            PreparedStatementController().prepare_qs_stmt("filter_species", force=True)
        self.assertEqual(
            str(ctx.exception), "Repeated placeholder name: pk. All placeholders in a query must have unique names."
        )

    def test_filter_too_many_params(self):
        """
        Given an ORM query is prepared with a filter that has one or more placeholders
        When  the prepared statement is executed with keywoird arguments that do not match the given placeholder names
        Then  a ValueError will be raised
        And   the error message will be "Unknown parameters supplied for prepared statement: {}"
        """

        def filter_species():
            return Species.prepare.filter(Q(pk=self.crow.pk) | Q(pk=Placeholder("pk"))).order_by("pk")

        PreparedStatementController().register_qs("filter_species", filter_species)
        PreparedStatementController().prepare_qs_stmt("filter_species", force=True)

        with self.assertRaises(ValueError) as ctx:
            qs = execute_stmt("filter_species", pk=1, pk2=2, pk3=3)
        self.assertEqual(str(ctx.exception), "Unknown parameters supplied for prepared statement: pk2 , pk3")

    def test_prepare_get(self):
        """
        Given an ORM query is prepared with a get() function
        When  the prepared statement is executed
        Then  a single model instance will be returned
        """

        def get_species():
            return Species.prepare.get(name=Placeholder("name"))

        PreparedStatementController().register_qs("get_species", get_species)
        PreparedStatementController().prepare_qs_stmt("get_species", force=True)

        qs = execute_stmt("get_species", name="Carp")

        self.assertTrue(isinstance(qs, Species))
        self.assertEqual(qs.name, self.carp.name)

    def test_prepare_first(self):
        """
        Given an ORM query is prepared with a first() function
        When  the prepared statement is executed
        Then  a single model instance will be returned
        And   it will be the model instance with the lowest primary key value
        """

        def first_species():
            return Species.prepare.first()

        PreparedStatementController().register_qs("first", first_species)
        PreparedStatementController().prepare_qs_stmt("first", force=True)

        qs = execute_stmt("first")

        self.assertTrue(isinstance(qs, Species))
        self.assertEqual(qs.name, self.tiger.name)

    def test_prepare_last(self):
        """
        Given an ORM query is prepared with a last() function
        When  the prepared statement is executed
        Then  a single model instance will be returned
        And   it will be the model instance with the highest primary key value
        """

        def last_species():
            return Species.prepare.last()

        PreparedStatementController().register_qs("last", last_species)
        PreparedStatementController().prepare_qs_stmt("last", force=True)

        qs = execute_stmt("last")

        self.assertTrue(isinstance(qs, Species))
        self.assertEqual(qs.name, self.crow.name)

    def test_prepare_count(self):
        """
        Given an ORM query is prepared with a count() function
        When  the prepared statement is executed
        Then  an integer value will be returned
        And   it will be the number of rows that match the query
        """

        def count_species():
            return Species.prepare.count()

        PreparedStatementController().register_qs("count", count_species)
        PreparedStatementController().prepare_qs_stmt("count", force=True)

        qs = execute_stmt("count")

        self.assertTrue(isinstance(qs, int))
        self.assertEqual(qs, 3)

    def test_prepare_prefetch_related(self):
        """
        Given an ORM query is written with a prefetch_related() function
        When  the query is prepared
        Then  a PreparedQueryNotSupported error will be raised
        """

        def will_fail():
            return Species.prepare.all().prefetch_related("animal_set")

        PreparedStatementController().register_qs("will_fail", will_fail)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

    def test_filtering_prepared_stmt_result(self):
        """
        Given an ORM query is prepared
        And   it has been succesfully executed
        When  any of the functions filter(), get(), latest() or earliest() are called on the resulting query set
        Then  a CannotAlterPreparedStatementQuerySet will be raised
        """
        PreparedStatementController().register_qs("all_species", lambda: Species.prepare.all())
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)
        qs = execute_stmt("all_species")

        with self.assertRaises(CannotAlterPreparedStatementQuerySet):
            qs.filter(id=1)

        with self.assertRaises(CannotAlterPreparedStatementQuerySet):
            qs.get(id=1)

        with self.assertRaises(CannotAlterPreparedStatementQuerySet):
            qs.latest("id")

        with self.assertRaises(CannotAlterPreparedStatementQuerySet):
            qs.earliest("id")

    def test_count_prepared_stmt_result(self):
        """
        Given an ORM query is prepared
        And   it has been succesfully executed
        When  count() is called on the resulting query set
        Then  the number of rows in the query set is returned
        """
        PreparedStatementController().register_qs("all_species", lambda: Species.prepare.all())
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)
        qs = execute_stmt("all_species")

        self.assertEqual(qs.count(), 3)

    def test_first_prepared_stmt_result(self):
        """
        Given an ORM query is prepared
        And   it has been succesfully executed
        When  first() is called on the resulting query set
        Then  the first record relative to the ordering of the prepared query is returned as a model instance
        """
        PreparedStatementController().register_qs("all_species", lambda: Species.prepare.all().order_by("pk"))
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)
        qs = execute_stmt("all_species")

        first = qs.first()
        self.assertEqual(first.name, self.tiger.name)

    def test_last_prepared_stmt_result(self):
        """
        Given an ORM query is prepared
        And   it has been succesfully executed
        When  last() is called on the resulting query set
        Then  the last record relative to the ordering of the prepared query is returned as a model instance
        """
        PreparedStatementController().register_qs("all_species", lambda: Species.prepare.all().order_by("pk"))
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)
        qs = execute_stmt("all_species")

        last = qs.last()
        self.assertEqual(last.name, self.crow.name)

    def test_prefetch_related_on_result(self):
        """
        Given an ORM query is prepared
        And   it has been succesfully executed
        When  prefetch_related() is called on the resulting query set
        Then  an extra query will be run to prefetvh the related objects
        And   no further queries will be run when the related objects are accessed from the original query set
        """
        Animal.objects.update_or_create(name="Tony", species=self.tiger)
        Animal.objects.update_or_create(name="Sheer Kahn", species=self.tiger)

        def qry():
            return Species.prepare.filter(name=Placeholder("name"))

        PreparedStatementController().register_qs("qry", qry)
        PreparedStatementController().prepare_qs_stmt("qry", force=True)

        qs = execute_stmt("qry", name="Tiger")

        # Now add the prefetch related, which should execute one query.
        with self.assertNumQueries(1):
            qs = qs.prefetch_related("animal_set")

        # Access the animals on the tiger species - as they are prefetched no more queries should be run!
        with self.assertNumQueries(0):
            tigers = qs[0].animal_set.all()
            self.assertEqual(len(tigers), 2)
            self.assertEqual(set([tigers[0].name, tigers[1].name]), set(["Tony", "Sheer Kahn"]))

    def test_prepare_values_list(self):
        """
        Given an ORM query is prepared with a values_list() function
        When  the query is prepared
        Then  an PreparedQueryNotSupported exception should be raised
        """
        def all_species():
            return Species.prepare.order_by("pk").values_list("name", flat=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("all_species", all_species)
            PreparedStatementController().prepare_qs_stmt("all_species", force=True)

    def test_values_list_on_result(self):
        """
        Given an ORM query is  preapred and executed
        When  .values_list() is called on the resulting query set
        And   flat=True
        Then  only the requested values should be returned in a flattened list
        """

        def all_species():
            return Species.prepare.order_by("pk")

        PreparedStatementController().register_qs("all_species", all_species)
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)

        qs = execute_stmt("all_species")
        qs = qs.values_list("name", flat=True)

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 3)
        self.assertEqual(qs[0], self.tiger.name)
        self.assertEqual(qs[1], self.carp.name)
        self.assertEqual(qs[2], self.crow.name)

    def test_related_values_list_on_result(self):
        """
        Given an ORM query is  preapred and executed
        When  .values_list() is called on the resulting query set
        And   the named parameter is on a related model
        And   flat=True
        Then  only the requested values should be returned in a flattened list
        """

        Animal.objects.update_or_create(name="Tony", species=self.tiger)
        Animal.objects.update_or_create(name="Sheer Kahn", species=self.tiger)

        def all_animal_species():
            return Animal.prepare.order_by("pk") #.values_list("species_id", flat=True)

        PreparedStatementController().register_qs("all_animal_species", all_animal_species)
        PreparedStatementController().prepare_qs_stmt("all_animal_species", force=True)

        qs = execute_stmt("all_animal_species")
        qs = qs.values_list("species_id", flat=True)

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 2)
        self.assertEqual(qs[0], self.tiger.id)
        self.assertEqual(qs[1], self.tiger.id)

    def test_named_values_list_on_result(self):
        """
        Given an ORM query is  preapred and executed
        When  .values_list() is called on the resulting query set
        And   named=True
        Then  only the requested values should be returned as a list of named tuples
        """

        def all_species():
            return Species.prepare.order_by("pk")

        PreparedStatementController().register_qs("all_species", all_species)
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)

        qs = execute_stmt("all_species")
        qs = qs.values_list("name", named=True)

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 3)
        self.assertEqual(qs[0].name, self.tiger.name)
        self.assertEqual(qs[1].name, self.carp.name)
        self.assertEqual(qs[2].name, self.crow.name)

    def test_not_supported_queryset_methods(self):
        """
        Given an ORM query is created using any of aggregate(), in_bulk(), create(), bulk_create(), bulk_update(),
              get_or_create(), update_or_create(), delete(), update(), exists() or explain()
        When  the query is prepared
        Then  a PreparedQueryNotSupported error will be raised
        """
        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.aggregate())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.in_bulk())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.create())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.bulk_create())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.bulk_update())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.get_or_create())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.update_or_create())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.delete())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.update())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.exists())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().register_qs("will_fail", lambda: Species.prepare.explain())
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)
