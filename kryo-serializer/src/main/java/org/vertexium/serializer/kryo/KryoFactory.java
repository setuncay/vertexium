package org.vertexium.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.vertexium.VertexiumException;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.type.GeoCircle;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;

import java.util.Date;
import java.util.HashMap;

public class KryoFactory {
    public Kryo createKryo() {
        Kryo kryo = new Kryo(new DefaultClassResolver(), new MapReferenceResolver() {
            @Override
            public boolean useReferences(Class type) {
                // avoid calling System.identityHashCode
                if (type == String.class || type == Date.class) {
                    return false;
                }
                return super.useReferences(type);
            }
        });
        registerClasses(kryo);

        kryo.setAutoReset(true);
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        return kryo;
    }

    private void registerClasses(Kryo kryo) {
        kryo.register(GeoPoint.class, 1001);
        kryo.register(HashMap.class, 1002);
        kryo.register(StreamingPropertyValueRef.class, 1003);
        kryo.register(GeoRect.class, 1006);
        kryo.register(GeoCircle.class, 1007);
        kryo.register(Date.class, 1008);
        registerAccumuloClasses(kryo);
        registerSqlClasses(kryo);
    }

    private void registerAccumuloClasses(Kryo kryo) {
        try {
            Class.forName("org.vertexium.accumulo.AccumuloGraph");
        } catch (ClassNotFoundException e) {
            // this is ok and expected if Accumulo is not in the classpath
            return;
        }
        try {
            kryo.register(Class.forName("org.vertexium.accumulo.iterator.model.EdgeInfo"), 1000);
            kryo.register(Class.forName("org.vertexium.accumulo.StreamingPropertyValueTableRef"), 1004);
            kryo.register(Class.forName("org.vertexium.accumulo.StreamingPropertyValueHdfsRef"), 1005);
        } catch (ClassNotFoundException ex) {
            throw new VertexiumException("Could not find accumulo classes to serialize", ex);
        }
    }

    private void registerSqlClasses(Kryo kryo) {
        try {
            Class.forName("org.vertexium.sql.SqlGraph");
        } catch (ClassNotFoundException e) {
            // this is ok and expected if SqlGraph is not in the classpath
            return;
        }
        try {
            kryo.register(Class.forName("org.vertexium.sql.models.EdgeInfoValue"), 2000);
            kryo.register(Class.forName("org.vertexium.sql.models.EdgeInHiddenValue"), 2001);
            kryo.register(Class.forName("org.vertexium.sql.models.EdgeInOutHiddenValue"), 2002);
            kryo.register(Class.forName("org.vertexium.sql.models.EdgeInOutVisibleValue"), 2003);
            kryo.register(Class.forName("org.vertexium.sql.models.EdgeInVisibleValue"), 2004);
            kryo.register(Class.forName("org.vertexium.sql.models.EdgeOutHiddenValue"), 2005);
            kryo.register(Class.forName("org.vertexium.sql.models.EdgeOutVisibleValue"), 2006);
            kryo.register(Class.forName("org.vertexium.sql.models.EdgeSignalValue"), 2007);
            kryo.register(Class.forName("org.vertexium.sql.models.ElementHiddenValue"), 2008);
            kryo.register(Class.forName("org.vertexium.sql.models.ElementSignalValueBase"), 2009);
            kryo.register(Class.forName("org.vertexium.sql.models.ElementVisibleValue"), 2010);
            kryo.register(Class.forName("org.vertexium.sql.models.PropertyHiddenValue"), 2011);
            kryo.register(Class.forName("org.vertexium.sql.models.PropertyMetadataValue"), 2012);
            kryo.register(Class.forName("org.vertexium.sql.models.PropertySoftDeleteValue"), 2013);
            kryo.register(Class.forName("org.vertexium.sql.models.PropertyValueBase"), 2014);
            kryo.register(Class.forName("org.vertexium.sql.models.PropertyValueValue"), 2015);
            kryo.register(Class.forName("org.vertexium.sql.models.PropertyVisibleValue"), 2016);
            kryo.register(Class.forName("org.vertexium.sql.models.SoftDeleteEdgeValue"), 2017);
            kryo.register(Class.forName("org.vertexium.sql.models.SoftDeleteElementValue"), 2018);
            kryo.register(Class.forName("org.vertexium.sql.models.SoftDeleteInEdgeValue"), 2019);
            kryo.register(Class.forName("org.vertexium.sql.models.SoftDeleteInOutEdgeValue"), 2020);
            kryo.register(Class.forName("org.vertexium.sql.models.SoftDeleteOutEdgeValue"), 2021);
            kryo.register(Class.forName("org.vertexium.sql.models.SoftDeleteVertexValue"), 2022);
            kryo.register(Class.forName("org.vertexium.sql.models.SqlGraphValueBase"), 2023);
            kryo.register(Class.forName("org.vertexium.sql.models.SqlStreamingPropertyValue"), 2024);
            kryo.register(Class.forName("org.vertexium.sql.models.SqlStreamingPropertyValueRef"), 2025);
            kryo.register(Class.forName("org.vertexium.sql.models.VertexSignalValue"), 2026);
            kryo.register(Class.forName("org.vertexium.sql.models.VertexTableEdgeValueBase"), 2027);

        } catch (ClassNotFoundException ex) {
            throw new VertexiumException("Could not find sql classes to serialize", ex);
        }
    }
}
