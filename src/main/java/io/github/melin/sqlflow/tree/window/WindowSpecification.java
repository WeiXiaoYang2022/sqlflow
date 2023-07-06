package io.github.melin.sqlflow.tree.window;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.*;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.OrderBy;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:45 PM
 */
public class WindowSpecification extends Node implements Window {
    private final Optional<Identifier> existingWindowName;
    private final List<Expression> partitionBy;
    private final Optional<OrderBy> orderBy;
    private final Optional<WindowFrame> frame;

    public WindowSpecification(Optional<Identifier> existingWindowName, List<Expression> partitionBy, Optional<OrderBy> orderBy, Optional<WindowFrame> frame) {
        this(Optional.empty(), existingWindowName, partitionBy, orderBy, frame);
    }

    public WindowSpecification(NodeLocation location, Optional<Identifier> existingWindowName, List<Expression> partitionBy, Optional<OrderBy> orderBy, Optional<WindowFrame> frame) {
        this(Optional.of(location), existingWindowName, partitionBy, orderBy, frame);
    }

    private WindowSpecification(Optional<NodeLocation> location, Optional<Identifier> existingWindowName, List<Expression> partitionBy, Optional<OrderBy> orderBy, Optional<WindowFrame> frame) {
        super(location);
        this.existingWindowName = requireNonNull(existingWindowName, "existingWindowName is null");
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.frame = requireNonNull(frame, "frame is null");
    }

    public Optional<Identifier> getExistingWindowName() {
        return existingWindowName;
    }

    public List<Expression> getPartitionBy() {
        return partitionBy;
    }

    public Optional<OrderBy> getOrderBy() {
        return orderBy;
    }

    public Optional<WindowFrame> getFrame() {
        return frame;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitWindowSpecification(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        existingWindowName.ifPresent(nodes::add);
        nodes.addAll(partitionBy);
        orderBy.ifPresent(nodes::add);
        frame.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        WindowSpecification o = (WindowSpecification) obj;
        return Objects.equals(existingWindowName, o.existingWindowName) &&
                Objects.equals(partitionBy, o.partitionBy) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(frame, o.frame);
    }

    @Override
    public int hashCode() {
        return Objects.hash(existingWindowName, partitionBy, orderBy, frame);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("existingWindowName", existingWindowName)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy)
                .add("frame", frame)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
