from django.db import models

class EntidadModel(models.Model):
    id = models.BigAutoField(primary_key=True)  
    Nit = models.BigIntegerField()
    Dv = models.SmallIntegerField(null=True, blank=True)  
    RazonSocial = models.CharField(max_length=150, null=True, blank=True) 
    TipoEntidad = models.SmallIntegerField(null=True, blank=True) 
    CodigoSuper = models.SmallIntegerField(null=True, blank=True) 
    Sigla = models.CharField(max_length=60, null=True, blank=True)
    Descripcion = models.CharField(max_length=60, null=True, blank=True)
    Departamento = models.CharField(max_length=30, null=True, blank=True)
    Ciudad = models.CharField(max_length=30, null=True, blank=True)
    Direccion = models.CharField(max_length=60, null=True, blank=True)
    Telefono = models.CharField(max_length=20, null=True, blank=True)
    Email = models.CharField(max_length=60, null=True, blank=True)
    CIIU = models.SmallIntegerField(null=True, blank=True)
    RepresentanteLegal = models.CharField(max_length=60, null=True, blank=True)
    Gremio = models.SmallIntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    class Meta:
        db_table = "entidad"
        ordering = ['-created_at']
