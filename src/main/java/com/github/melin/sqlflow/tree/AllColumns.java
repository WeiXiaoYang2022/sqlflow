package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/18 11:19 PM
 */
public class AllColumns extends SelectItem {
    private final List<Identifier> aliases;
    private final Optional<Expression> target;

    public AllColumns() {
        this(Optional.empty(), Optional.empty(), ImmutableList.of());
    }

    public AllColumns(Expression target) {
        this(Optional.empty(), Optional.of(target), ImmutableList.of());
    }

    public AllColumns(Expression target, List<Identifier> aliases) {
        this(Optional.empty(), Optional.of(target), aliases);
    }

    public AllColumns(NodeLocation location, Optional<Expression> target, List<Identifier> aliases) {
        this(Optional.of(location), target, aliases);
    }

    public AllColumns(Optional<NodeLocation> location, Optional<Expression> target, List<Identifier> aliases) {
        super(location);
        this.aliases = ImmutableList.copyOf(requireNonNull(aliases, "aliases is null"));
        this.target = requireNonNull(target, "target is null");
    }

    public List<Identifier> getAliases() {
        return aliases;
    }

    public Optional<Expression> getTarget() {
        return target;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAllColumns(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return target.map(ImmutableList::<Node>of)
                .orElse(ImmutableList.of());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AllColumns other = (AllColumns) o;
        return Objects.equals(aliases, other.aliases) &&
                Objects.equals(target, other.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases, target);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        target.ifPresent(value -> builder.append(value).append("."));
        builder.append("*");

        if (!aliases.isEmpty()) {
            builder.append(" (");
            Joiner.on(", ").appendTo(builder, aliases);
            builder.append(")");
        }

        return builder.toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        return aliases.equals(((AllColumns) other).aliases);
    }
}
