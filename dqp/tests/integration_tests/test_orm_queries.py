import warnings

from django.test import TestCase, TransactionTestCase

from dqp import execute_stmt, Placeholders
from dqp.tests.integration_tests.models import Species, Animal
from dqp.prepared_stmt_controller import PreparedStatementController
from dqp.queryset import PreparedStatementQuerySet
from dqp.exceptions import CannotAlterPreparedStatementQuerySet, PreparedQueryNotSupported


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
        def all_species():
            return Species.prepare.all().order_by("pk")

        PreparedStatementController().register_qs("all_species", all_species)
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)

        qs = execute_stmt("all_species")

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 3)
        self.assertTrue(isinstance(qs[0], Species))
        self.assertTrue(qs[0].name, self.tiger.name)

    def test_prepare_filter(self):
        def filter_species():
            return Species.prepare.filter(name__icontains=Placeholders.CharField)

        PreparedStatementController().register_qs("filter_species", filter_species)
        PreparedStatementController().prepare_qs_stmt("filter_species", force=True)

        # N.B. __icontains doesn't work the same as in normal django.
        # TODO: fix this (see below).
        qs = execute_stmt("filter_species", ["%car%"])

        self.assertTrue(isinstance(qs, PreparedStatementQuerySet))
        self.assertEqual(len(qs), 1)
        self.assertTrue(isinstance(qs[0], Species))
        self.assertTrue(qs[0].name, self.carp.name)

    def test_prepare_icontains(self):
        warnings.warn("test_prepare_icontains is not implemented")

    def test_filter_with_constant(self):
        warnings.warn("test_filter_with_constant is not implemented")
        # def filter_species():
        #     return Species.prepare.filter(pk=self.crow.pk)
        #
        # PreparedStatementController().register_qs("filter_species", filter_species)
        # PreparedStatementController().prepare_qs_stmt("filter_species", force=True)
        #
        # # This currently breaks because self.crow.pk has been forgotten and the execute statement expects a parameter
        # # to be passed in!
        # qs = execute_stmt("filter_species")
        #
        # self.assertTrue(qs[0].name, self.crow.name)

    def test_prepare_get(self):
        def get_species():
            return Species.prepare.get(name=Placeholders.CharField)

        PreparedStatementController().register_qs("get_species", get_species)
        PreparedStatementController().prepare_qs_stmt("get_species", force=True)

        qs = execute_stmt("get_species", ["Carp"])

        self.assertTrue(isinstance(qs, Species))
        self.assertTrue(qs.name, self.tiger.name)

    def test_prepare_first(self):
        def first_species():
            return Species.prepare.first()

        PreparedStatementController().register_qs("first", first_species)
        PreparedStatementController().prepare_qs_stmt("first", force=True)

        qs = execute_stmt("first")

        self.assertTrue(isinstance(qs, Species))
        self.assertTrue(qs.name, self.tiger.name)

    def test_prepare_last(self):
        def last_species():
            return Species.prepare.last()

        PreparedStatementController().register_qs("last", last_species)
        PreparedStatementController().prepare_qs_stmt("last", force=True)

        qs = execute_stmt("last")

        self.assertTrue(isinstance(qs, Species))
        self.assertTrue(qs.name, self.crow.name)

    def test_prepare_count(self):
        def count_species():
            return Species.prepare.count()

        PreparedStatementController().register_qs("count", count_species)
        PreparedStatementController().prepare_qs_stmt("count", force=True)

        qs = execute_stmt("count")

        self.assertTrue(isinstance(qs, int))
        self.assertEqual(qs, 3)

    def test_prepare_prefetch_related(self):
        def will_fail():
            return Species.prepare.all().prefetch_related("animal_set")

        PreparedStatementController().register_qs("will_fail", will_fail)

        with self.assertRaises(PreparedQueryNotSupported):
            PreparedStatementController().prepare_qs_stmt("will_fail", force=True)

    def test_filtering_prepared_stmt_result(self):
        """
        Cannot use filter, get, latest or earliest on PreparedStatementQuerySet objects
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
        """
        PreparedStatementController().register_qs("all_species", lambda: Species.prepare.all())
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)
        qs = execute_stmt("all_species")

        self.assertEqual(qs.count(), 3)

    def test_first_prepared_stmt_result(self):
        """
        """
        PreparedStatementController().register_qs("all_species", lambda: Species.prepare.all().order_by("pk"))
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)
        qs = execute_stmt("all_species")

        first = qs.first()
        self.assertTrue(first.name, self.tiger.name)

    def test_last_prepared_stmt_result(self):
        """
        """
        PreparedStatementController().register_qs("all_species", lambda: Species.prepare.all().order_by("pk"))
        PreparedStatementController().prepare_qs_stmt("all_species", force=True)
        qs = execute_stmt("all_species")

        last = qs.last()
        self.assertTrue(last.name, self.crow.name)

    def test_prepare_prefetch_related(self):

        Animal.objects.update_or_create(name="Tony", species=self.tiger)
        Animal.objects.update_or_create(name="Sheer Kahn", species=self.tiger)

        def qry():
            return Species.prepare.filter(name=Placeholders.CharField)

        PreparedStatementController().register_qs("qry", qry)
        PreparedStatementController().prepare_qs_stmt("qry", force=True)

        qs = execute_stmt("qry", ["Tiger"])

        # Now add the prefetch related, which should execute one query.
        with self.assertNumQueries(1):
            qs = qs.prefetch_related("animal_set")

        # Access the animals on the tiger species - as they are prefetched no more queries should be run!
        with self.assertNumQueries(0):
            tigers = qs[0].animal_set.all()
            self.assertEqual(len(tigers), 2)
            self.assertEqual(set([tigers[0].name, tigers[1].name]), set(["Tony", "Sheer Kahn"]))

    def test_not_supported_queryset_methods(self):
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
