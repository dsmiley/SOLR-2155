Author: David Smiley

Instructions:

   schema.xml:
        <fieldType name="geohash" class="solr2155.solr.schema.GeoHashField" length="12" />

        <field name="store" type="geohash" indexed="true" stored="true" multiValued="true"/>

   solrconfig.xml:
      Top level within <config>, suggested to place at bottom:
        <!-- an alternative query parser to geofilt() (notably allows a specific lat-lon box) -->
        <queryParser name="gh_geofilt" class="solr2155.solr.search.SpatialGeoHashFilterQParser$Plugin" />
        <!-- replace built-in geodist() with our own modified one -->
        <valueSourceParser name="geodist" class="solr2155.solr.search.function.distance.HaversineConstFunction$HaversineValueSourceParser" />
        
      Add the following cache into <query> section if you are going to use geodist func
          <cache name="fieldValueCache"
            class="solr.FastLRUCache"
            size="10"
            initialSize="1"
            autowarmCount="1"/>

