package net.myvst.v2.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Set;


/**
 * 统计 count, countDistinct
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class StatCounter implements Serializable {
    private int count;
    private Set<String> countDistinct;
}
