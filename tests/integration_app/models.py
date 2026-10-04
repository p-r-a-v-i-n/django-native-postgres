from django.db import models


class Book(models.Model):
    name = models.CharField(max_length=200)


def set_null():
    return None


class DeletionParent(models.Model):
    name = models.CharField(max_length=200)


class CascadeChild(models.Model):
    parent = models.ForeignKey(DeletionParent, on_delete=models.CASCADE)


class CascadeGrandchild(models.Model):
    parent = models.ForeignKey(CascadeChild, on_delete=models.CASCADE)


class ProtectedGrandchild(models.Model):
    parent = models.ForeignKey(CascadeChild, on_delete=models.PROTECT)


class NullableChild(models.Model):
    parent = models.ForeignKey(
        DeletionParent,
        null=True,
        on_delete=models.SET_NULL,
    )


class CallableSetChild(models.Model):
    parent = models.ForeignKey(
        DeletionParent,
        null=True,
        on_delete=models.SET(set_null),
    )


class ProtectedChild(models.Model):
    parent = models.ForeignKey(DeletionParent, on_delete=models.PROTECT)


class RestrictedChild(models.Model):
    parent = models.ForeignKey(DeletionParent, on_delete=models.RESTRICT)
