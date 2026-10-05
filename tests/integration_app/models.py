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


class UniqueRecord(models.Model):
    key = models.CharField(max_length=200, unique=True)
    value = models.CharField(max_length=200)


class Tag(models.Model):
    name = models.CharField(max_length=200, unique=True)
    value = models.CharField(max_length=200, default="")


class Article(models.Model):
    title = models.CharField(max_length=200)
    tags = models.ManyToManyField(Tag, related_name="articles")


class Person(models.Model):
    name = models.CharField(max_length=200)
    friends = models.ManyToManyField("self")


class Member(models.Model):
    name = models.CharField(max_length=200)


class Club(models.Model):
    name = models.CharField(max_length=200)
    members = models.ManyToManyField(Member, through="Membership")


class Membership(models.Model):
    club = models.ForeignKey(Club, on_delete=models.CASCADE)
    member = models.ForeignKey(Member, on_delete=models.CASCADE)
    role = models.CharField(max_length=200)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["club", "member"],
                name="unique_club_member",
            )
        ]
