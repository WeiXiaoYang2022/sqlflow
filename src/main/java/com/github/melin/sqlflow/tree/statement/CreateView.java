package com.github.melin.sqlflow.tree.statement;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 12:46 PM
 */
public class CreateView extends Statement {
    public enum Security {
        INVOKER, DEFINER
    }

    private final QualifiedName name;
    private final Query query;
    private final boolean replace;
    private final Optional<String> comment;
    private final Optional<Security> security;

    public CreateView(QualifiedName name, Query query, boolean replace, Optional<String> comment, Optional<Security> security) {
        this(Optional.empty(), name, query, replace, comment, security);
    }

    public CreateView(NodeLocation location, QualifiedName name, Query query, boolean replace, Optional<String> comment, Optional<Security> security) {
        this(Optional.of(location), name, query, replace, comment, security);
    }

    private CreateView(Optional<NodeLocation> location, QualifiedName name, Query query, boolean replace, Optional<String> comment, Optional<Security> security) {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.replace = replace;
        this.comment = requireNonNull(comment, "comment is null");
        this.security = requireNonNull(security, "security is null");
    }

    public QualifiedName getName() {
        return name;
    }

    public Query getQuery() {
        return query;
    }

    public boolean isReplace() {
        return replace;
    }

    public Optional<String> getComment() {
        return comment;
    }

    public Optional<Security> getSecurity() {
        return security;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateView(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, replace, security);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateView o = (CreateView) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(query, o.query)
                && Objects.equals(replace, o.replace)
                && Objects.equals(comment, o.comment)
                && Objects.equals(security, o.security);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("replace", replace)
                .add("comment", comment)
                .add("security", security)
                .toString();
    }
}
