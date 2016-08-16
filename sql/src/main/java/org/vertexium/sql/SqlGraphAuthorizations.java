package org.vertexium.sql;

import org.vertexium.VertexiumException;
import org.vertexium.Visibility;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;
import org.vertexium.util.ArrayUtils;
import org.vertexium.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;

public class SqlGraphAuthorizations implements org.vertexium.Authorizations, Serializable {
    private static final long serialVersionUID = 1L;
    private final String[] authorizations;

    public SqlGraphAuthorizations(String... authorizations) {
        this.authorizations = authorizations;
    }

    @Override
    public String[] getAuthorizations() {
        return authorizations;
    }

    @Override
    public boolean equals(org.vertexium.Authorizations authorizations) {
        return ArrayUtils.intersectsAll(getAuthorizations(), authorizations.getAuthorizations());
    }

    @Override
    public String toString() {
        return Arrays.toString(authorizations);
    }

    @Override
    public boolean canRead(Visibility visibility) {
        Preconditions.checkNotNull(visibility, "visibility is required");

        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        if (visibility.getVisibilityString().length() == 0) {
            return true;
        }

        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(this.getAuthorizations());
        ColumnVisibility columnVisibility = new ColumnVisibility(visibility.getVisibilityString());
        try {
            return visibilityEvaluator.evaluate(columnVisibility);
        } catch (VisibilityParseException e) {
            throw new VertexiumException("could not evaluate visibility " + visibility.getVisibilityString(), e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlGraphAuthorizations that = (SqlGraphAuthorizations) o;

        return ArrayUtils.intersectsAll(getAuthorizations(), that.getAuthorizations());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(authorizations);
    }
}